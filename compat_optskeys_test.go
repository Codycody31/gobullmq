package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func errFake(i int) error { return fmt.Errorf("fail %d", i) }

// Upstream stores shortened option keys (optsAsJSON: failParentOnFailure->fpof,
// keepLogs->kl, removeDependencyOnFailure->rdof) in the job opts JSON and the
// msgpack opts. Go must write and read the same short keys.
func TestOptsUseUpstreamShortKeys(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	job, err := q.Add(ctx, "test", map[string]any{},
		AddWithFailParentOnFailure(true),
		AddWithRemoveDependencyOnFailure(true),
		AddWithKeepLogs(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, err := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	if err != nil {
		t.Fatal(err)
	}
	var opts map[string]any
	if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
		t.Fatal(err)
	}
	if opts["fpof"] != true || opts["rdof"] != true || opts["kl"] != float64(3) {
		t.Fatalf("stored opts lack short keys: %s", optsJSON)
	}
	for _, long := range []string{"failParentOnFailure", "removeDependencyOnFailure", "keepLogs"} {
		if _, has := opts[long]; has {
			t.Fatalf("stored opts contain long key %q: %s", long, optsJSON)
		}
	}

	// A JS-written opts blob must parse into the Go fields.
	parsed, err := jobOptsFromJson(`{"attempts":0,"delay":0,"fpof":true,"kl":5,"rdof":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.FailParentOnFailure || !parsed.RemoveDependencyOnFailure || parsed.KeepLogs != 5 {
		t.Fatalf("JS opts blob not parsed: %+v", parsed)
	}
}

// keepLogs (kl) caps the job's log list on every Log call (job.ts:393-411).
func TestKeepLogsTrimsJobLogs(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)

	job, err := q.Add(ctx, "test", map[string]any{}, AddWithKeepLogs(2))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"one", "two", "three"} {
		n, err := job.Log(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		if n > 2 {
			t.Fatalf("Log returned count %d beyond keepLogs", n)
		}
	}
	logs, total, err := job.Logs(ctx, 0, -1)
	if err != nil || total != 2 {
		t.Fatalf("logs = %v total %d (%v), want the last 2", logs, total, err)
	}
	if logs[0] != "two" || logs[1] != "three" {
		t.Fatalf("kept wrong entries: %v", logs)
	}
}

// stackTraceLimit caps the stacktrace array kept on failures (job.ts:1153-1154,
// upstream keeps the first N entries).
func TestStackTraceLimit(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "test", map[string]any{}, AddWithAttempts(5), AddWithStackTraceLimit(1))
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	// Two failing attempts accumulate two entries; the limit keeps one.
	for i := 0; i < 2; i++ {
		token := w.token.String() + ":" + strings.Repeat("y", i+1)
		fetched, err := w.moveToActive(ctx, token, "")
		if err != nil || fetched == nil {
			t.Fatalf("moveToActive attempt %d: %v %v", i, fetched, err)
		}
		w.handleJobError(*fetched, token, errFake(i))
	}

	raw, err := c.HGet(ctx, prefix+job.ID(), "stacktrace").Result()
	if err != nil {
		t.Fatal(err)
	}
	var st []string
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 {
		t.Fatalf("stacktrace has %d entries, want 1 (stackTraceLimit): %v", len(st), st)
	}
}

// sizeLimit rejects oversized job data at add time (job.ts:1111-1116).
func TestSizeLimitRejectsOversizedData(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)

	_, err := q.Add(ctx, "test", map[string]any{"blob": strings.Repeat("x", 100)}, AddWithSizeLimit(10))
	if err == nil || !strings.Contains(err.Error(), "exceeds the limit") {
		t.Fatalf("oversized data accepted (err=%v); upstream throws", err)
	}
	if _, err := q.Add(ctx, "test", map[string]any{"k": 1}, AddWithSizeLimit(1000)); err != nil {
		t.Fatalf("small data rejected: %v", err)
	}
}

// DefaultJobOptions merge under per-call options, like upstream jobsOpts.
func TestQueueDefaultJobOptions(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, &QueueOptions{
		DefaultJobOptions: &JobOptions{Attempts: 5, Backoff: &BackoffOptions{Type: "fixed", Delay: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}

	job, err := q.Add(ctx, "defaults", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, _ := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	if !strings.Contains(optsJSON, `"attempts":5`) || !strings.Contains(optsJSON, `"type":"fixed"`) {
		t.Fatalf("defaults not applied: %s", optsJSON)
	}

	// Per-call options override defaults.
	job2, err := q.Add(ctx, "override", map[string]any{}, AddWithAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, _ = c.HGet(ctx, prefix+job2.ID(), "opts").Result()
	if !strings.Contains(optsJSON, `"attempts":2`) {
		t.Fatalf("per-call option did not override default: %s", optsJSON)
	}
}

// Worker emits 'closed' when close completes (worker.ts:784).
func TestWorkerClosedEvent(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)

	w, err := NewWorker[map[string]any, string](name, c,
		func(ctx context.Context, job *Job[map[string]any]) (string, error) { return "", nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{}, 1)
	w.On("closed", func(...any) { closed <- struct{}{} })

	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("no 'closed' event emitted by Close")
	}
}
