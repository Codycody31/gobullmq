package gobullmq

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock renewal must target <prefix><jobId>:lock, and a failed renewal must surface.
func TestExtendLockUsesPerJobLockKey(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	// Simulate what moveToActive does: a per-job lock owned by our token.
	lockKey := prefix + "42:lock"
	if err := c.Set(ctx, lockKey, "tok", 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	if err := jobExtendLock(ctx, c, prefix, "42", "tok", 60_000); err != nil {
		t.Fatalf("jobExtendLock: %v", err)
	}

	ttl, err := c.PTTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 30*time.Second {
		t.Fatalf("lock was not renewed: PTTL=%v, want ~60s", ttl)
	}
}

func TestWorkerExtendLocksForJobs(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	lockKey := prefix + "7:lock"
	if err := c.Set(ctx, lockKey, "tok7", 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	if err := w.extendLocksForJobs([]*rawJob{{id: "7", token: "tok7"}}); err != nil {
		t.Fatalf("extendLocksForJobs: %v", err)
	}
	ttl, err := c.PTTL(ctx, lockKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 20*time.Second {
		t.Fatalf("worker lock renewal ineffective: PTTL=%v, want ~LockDuration (30s)", ttl)
	}

	// A renewal for a lock owned by someone else must report failure.
	if err := c.Set(ctx, lockKey, "other-owner", 5*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if err := w.extendLocksForJobs([]*rawJob{{id: "7", token: "tok7"}}); err == nil {
		t.Fatal("extendLocksForJobs succeeded on a lock owned by another token; want error")
	}
}

// Jobs loaded by ID must carry their ID.
func TestGetJobPopulatesID(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)

	added, err := q.Add(ctx, "test", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if added.ID() == "" {
		t.Fatal("Add returned job with empty ID")
	}

	got, err := q.Job(ctx, added.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("Job returned nil for existing job")
	}
	if got.ID() != added.ID() {
		t.Fatalf("Job ID = %q, want %q", got.ID(), added.ID())
	}
	state, err := got.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state != "waiting" {
		t.Fatalf("State = %q, want %q", state, "waiting")
	}
}

// Progress events must land on the queue events stream, not a per-job stream.
func TestUpdateProgressEmitsToQueueEventsStream(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	added, err := q.Add(ctx, "test", map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}

	s := newScripts(c, prefix)
	if err := s.updateProgress(ctx, added.ID(), 42); err != nil {
		t.Fatalf("updateProgress: %v", err)
	}

	if n, _ := c.XLen(ctx, prefix+added.ID()+":events").Result(); n != 0 {
		t.Fatalf("progress event written to phantom per-job stream (%d entries)", n)
	}

	msgs, err := c.XRange(ctx, prefix+"events", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range msgs {
		if m.Values["event"] == "progress" && m.Values["jobId"] == added.ID() {
			found = true
			if m.Values["data"] != "42" {
				t.Fatalf("progress data = %v, want 42", m.Values["data"])
			}
		}
	}
	if !found {
		t.Fatal("no progress event on queue events stream")
	}
}

// The parent option must be stored as a JSON object with id and queueKey.
func TestAddJobStoresParentAsObject(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	parent, err := q.Add(ctx, "parent", map[string]any{"p": true}, func(o *JobOptions) { o.WaitChildren = true })
	if err != nil {
		t.Fatal(err)
	}

	child, err := q.Add(ctx, "child", map[string]any{"c": true},
		AddWithParent(ParentOptions{ID: parent.ID(), Queue: strings.TrimSuffix(prefix, ":")}))
	if err != nil {
		t.Fatal(err)
	}

	rawParent, err := c.HGet(ctx, prefix+child.ID(), "parent").Result()
	if err != nil {
		t.Fatalf("child hash has no parent field: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(rawParent), &parsed); err != nil {
		t.Fatalf("parent field is not a JSON object: %q (%v)", rawParent, err)
	}
	if parsed["id"] != parent.ID() {
		t.Fatalf("parent.id = %v, want %q", parsed["id"], parent.ID())
	}
	if parsed["queueKey"] != strings.TrimSuffix(prefix, ":") {
		t.Fatalf("parent.queueKey = %v, want %q", parsed["queueKey"], strings.TrimSuffix(prefix, ":"))
	}
	// And the child must be registered as a dependency of the parent.
	deps, err := c.SMembers(ctx, prefix+parent.ID()+":dependencies").Result()
	if err != nil || len(deps) != 1 {
		t.Fatalf("parent dependencies = %v (err %v), want one entry", deps, err)
	}
}

// Repeatable jobs with EndDate must be addable, and dates stored as epoch ms.
func TestRepeatableJobWithEndDate(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	end := time.Now().Add(time.Hour).UnixMilli()
	job, err := q.Add(ctx, "rep", map[string]any{"r": 1}, AddWithRepeat(JobRepeatOptions{
		Every:   60_000,
		EndDate: end,
	}))
	if err != nil {
		t.Fatalf("Add repeatable with EndDate: %v", err)
	}

	optsJSON, err := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	if err != nil {
		t.Fatal(err)
	}
	var opts map[string]any
	if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
		t.Fatal(err)
	}
	rep, _ := opts["repeat"].(map[string]any)
	if rep == nil {
		t.Fatalf("stored opts have no repeat object: %s", optsJSON)
	}
	if _, ok := rep["endDate"].(float64); !ok {
		t.Fatalf("stored repeat.endDate is not a number: %v (%T)", rep["endDate"], rep["endDate"])
	}
}

// KeepJobs{Count: 0} means "remove immediately" and must serialize count.
func TestKeepJobsCountZeroSerialized(t *testing.T) {
	b, err := json.Marshal(KeepJobs{Count: 0})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"count":0`) {
		t.Fatalf("KeepJobs{Count:0} marshals to %s, want count:0 present", b)
	}

	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)
	job, err := q.Add(ctx, "test", map[string]any{}, AddWithRemoveOnComplete(KeepJobs{Count: 0}))
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, err := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(optsJSON, `"count":0`) {
		t.Fatalf("stored opts %s lack removeOnComplete count:0", optsJSON)
	}
}

// Custom job IDs may start with '0' but must not be '0' or start with '0:'.
func TestJobIDValidation(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)

	if _, err := q.Add(ctx, "test", map[string]any{}, AddWithJobID("01legal")); err != nil {
		t.Fatalf("legal jobId '01legal' rejected: %v", err)
	}
	if _, err := q.Add(ctx, "test", map[string]any{}, AddWithJobID("0")); err == nil {
		t.Fatal("jobId '0' accepted; must be rejected")
	}
	if _, err := q.Add(ctx, "test", map[string]any{}, AddWithJobID("0:123")); err == nil {
		t.Fatal("jobId '0:123' accepted; reserved delayed-marker namespace must be rejected")
	}
}

// Clean must use millisecond timestamps.
func TestCleanUsesMilliseconds(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	job, err := q.Add(ctx, "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a completion one minute ago (completed zset scores are epoch ms).
	if err := c.ZAdd(ctx, prefix+"completed", redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).UnixMilli()),
		Member: job.ID(),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	removed, err := q.Clean(ctx, 0, 0, JobStateCompleted)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != job.ID() {
		t.Fatalf("Clean removed %v, want [%s]", removed, job.ID())
	}
}

// Stalled jobs failed by the stalled checker must get ms-scale scores,
// and the default MaxStalledCount must be 1 (fail on the second stall, not the first).
func TestStalledJobFailureTimestampsAndDefault(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

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
	if w.opts.MaxStalledCount != 1 {
		t.Fatalf("MaxStalledCount default = %d, want 1", w.opts.MaxStalledCount)
	}

	// Move the job to active, then mark it stalled twice; with MaxStalledCount=1
	// the first check re-queues it, the second fails it.
	if _, err := w.moveToActive(ctx, w.token.String()+":1", ""); err != nil {
		t.Fatal(err)
	}
	// The stalled check only considers active jobs whose lock is gone.
	if err := c.Del(ctx, prefix+job.ID()+":lock").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.SAdd(ctx, prefix+"stalled", job.ID()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := w.moveStalledJobsToWait(); err != nil {
		t.Fatal(err)
	}
	// First stall: job must be back in wait, not failed.
	if n, _ := c.ZScore(ctx, prefix+"failed", job.ID()).Result(); n != 0 {
		t.Fatalf("job failed on first stall (score %v); default MaxStalledCount must allow one stall", n)
	}

	// Reactivate and stall again.
	if _, err := w.moveToActive(ctx, w.token.String()+":2", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.Del(ctx, prefix+job.ID()+":lock").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.SAdd(ctx, prefix+"stalled", job.ID()).Err(); err != nil {
		t.Fatal(err)
	}
	// The stalled-check key throttles runs to one per StalledInterval.
	if err := c.Del(ctx, prefix+"stalled-check").Err(); err != nil {
		t.Fatal(err)
	}
	if err := w.moveStalledJobsToWait(); err != nil {
		t.Fatal(err)
	}

	score, err := c.ZScore(ctx, prefix+"failed", job.ID()).Result()
	if err != nil {
		t.Fatalf("job not failed after exceeding MaxStalledCount: %v", err)
	}
	if score < 1e12 {
		t.Fatalf("failed zset score %v is not epoch milliseconds", score)
	}
	finishedOn, err := c.HGet(ctx, prefix+job.ID(), "finishedOn").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(finishedOn) < 13 {
		t.Fatalf("finishedOn %q is not epoch milliseconds", finishedOn)
	}
}
