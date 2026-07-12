package gobullmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An unset DrainDelay must default to 5s (upstream drainDelay: 5),
// not fall through to a 10ms busy-poll.
func TestDrainDelayDefault(t *testing.T) {
	c := testClient(t)
	name := testQueueName(t, c)
	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if w.opts.DrainDelay != 5*time.Second {
		t.Fatalf("DrainDelay default = %v, want 5s", w.opts.DrainDelay)
	}
}

// Moving a job to failed with a token that does not own the lock must
// surface the moveToFinished error code instead of silently succeeding.
func TestMoveToFailedSurfacesLockErrors(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	fetched, err := w.moveToActive(ctx, w.token.String()+":1", "")
	if err != nil || fetched == nil {
		t.Fatalf("moveToActive: %v %v", fetched, err)
	}
	if fetched.id != job.ID() {
		t.Fatalf("fetched %s, want %s", fetched.id, job.ID())
	}

	stacktrace, err := fetched.prepareStacktrace(errors.New("boom"))
	if err != nil {
		t.Fatal(err)
	}
	moveErr := jobMoveToFailed(ctx, w.scripts, fetched, stacktrace, "not-the-owner", false, 30000, "")
	if moveErr == nil {
		t.Fatal("jobMoveToFailed with wrong token succeeded; want lock error")
	}
}

// A paused worker must not pull new jobs, and Resume must restore fetching:
// fetch tasks have to survive the pause, or a resumed worker is left with
// zero fetchers and never processes again.
func TestWorkerPauseResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testClient(t)
	name := testQueueName(t, c)

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker[map[string]any, string](name, c,
		func(ctx context.Context, job *Job[map[string]any]) (string, error) { return "ok", nil },
		&WorkerOptions{Concurrency: 1, DrainDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Pause()
	time.Sleep(300 * time.Millisecond) // let in-flight fetches settle into the pause loop

	job, err := q.Add(ctx, "after-pause", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if s, _ := q.JobState(ctx, job.ID()); s != JobStateWaiting {
		t.Fatalf("job state while paused = %s, want waiting", s)
	}

	w.Resume()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if s, _ := q.JobState(ctx, job.ID()); s == JobStateCompleted {
			break
		}
		if time.Now().After(deadline) {
			s, _ := q.JobState(ctx, job.ID())
			t.Fatalf("resumed worker never processed the job (state %s)", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A discarded job must not be retried even with attempts remaining.
func TestDiscardPreventsRetry(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "test", map[string]any{}, AddWithAttempts(5))
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	token := w.token.String() + ":1"
	fetched, err := w.moveToActive(ctx, token, "")
	if err != nil || fetched == nil {
		t.Fatalf("moveToActive: %v %v", fetched, err)
	}

	fetched.discarded = true
	if _, err := w.handleJobError(*fetched, token, errors.New("boom")); err == nil {
		t.Fatal("handleJobError returned nil error")
	}

	if err := c.ZScore(ctx, prefix+"failed", job.ID()).Err(); err != nil {
		t.Fatalf("discarded job was not moved to failed: %v", err)
	}
	if n, _ := c.LLen(ctx, prefix+"wait").Result(); n != 0 {
		t.Fatal("discarded job was retried into wait")
	}
}

// ErrUnrecoverable fails the job immediately, skipping retries (upstream UnrecoverableError).
func TestUnrecoverableErrorSkipsRetries(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "test", map[string]any{}, AddWithAttempts(5))
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	token := w.token.String() + ":1"
	fetched, err := w.moveToActive(ctx, token, "")
	if err != nil || fetched == nil {
		t.Fatalf("moveToActive: %v %v", fetched, err)
	}

	procErr := errors.Join(ErrUnrecoverable, errors.New("corrupt payload"))
	if _, err := w.handleJobError(*fetched, token, procErr); err == nil {
		t.Fatal("handleJobError returned nil error")
	}

	if err := c.ZScore(ctx, prefix+"failed", job.ID()).Err(); err != nil {
		t.Fatalf("unrecoverable job was not moved to failed: %v", err)
	}
}
