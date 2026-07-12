package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Flows: added atomically, UUID job ids, parent promoted when children complete.
func TestFlowProducerEndToEnd(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	fp, err := NewFlowProducer(c, nil)
	if err != nil {
		t.Fatal(err)
	}

	tree, err := fp.Add(ctx, FlowJob{
		Name:      "parent",
		QueueName: name,
		Data:      map[string]any{"root": true},
		Children: []FlowJob{
			{Name: "child1", QueueName: name, Data: map[string]any{"n": 1}},
			{Name: "child2", QueueName: name, Data: map[string]any{"n": 2}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if tree.Job == nil || len(tree.Children) != 2 {
		t.Fatalf("flow result malformed: %+v", tree)
	}

	// Flow nodes get UUID v4 ids like upstream, not INCR counters.
	for _, id := range []string{tree.Job.ID(), tree.Children[0].Job.ID(), tree.Children[1].Job.ID()} {
		if len(id) != 36 || strings.Count(id, "-") != 4 {
			t.Fatalf("flow node id %q is not a UUID", id)
		}
	}

	// Parent must be in waiting-children with two dependencies.
	state, err := tree.Job.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state != "waiting-children" {
		t.Fatalf("parent state = %q, want waiting-children", state)
	}
	deps, _ := c.SMembers(ctx, prefix+tree.Job.ID()+":dependencies").Result()
	if len(deps) != 2 {
		t.Fatalf("parent has %d dependencies, want 2", len(deps))
	}

	// Complete both children through the real worker path; the parent must
	// then be promoted out of waiting-children (proves the packed parent
	// object satisfies moveToFinished's parent handling).
	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx
	for i := 0; i < 2; i++ {
		token := w.token.String() + ":" + strings.Repeat("x", i+1)
		fetched, err := w.moveToActive(ctx, token, "")
		if err != nil || fetched == nil {
			t.Fatalf("moveToActive child %d: %v %v", i, fetched, err)
		}
		if _, err := w.handleJobSuccess(*fetched, token, "ok", func() bool { return false }); err != nil {
			t.Fatalf("handleJobSuccess child %d: %v", i, err)
		}
	}

	state, err = tree.Job.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state != "waiting" {
		t.Fatalf("parent state after children completed = %q, want waiting", state)
	}

	// Flow returns the tree.
	got, err := fp.Flow(ctx, FlowGetOptions{ID: tree.Job.ID(), QueueName: name})
	if err != nil {
		t.Fatal(err)
	}
	if got.Job == nil || got.Job.ID() != tree.Job.ID() || len(got.Children) != 2 {
		t.Fatalf("Flow returned malformed tree: job=%v children=%d", got.Job, len(got.Children))
	}
}

// Worker.RateLimit establishes a rate-limit window like upstream worker.rateLimit(ms).
func TestWorkerRateLimit(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RateLimit(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	ttl, err := c.PTTL(ctx, prefix+"limiter").Result()
	if err != nil || ttl <= 50*time.Second {
		t.Fatalf("limiter TTL = %v (err %v), want ~60s", ttl, err)
	}
}

// Queue.Workers / EventListeners list clients by their SETNAME.
func TestGetWorkersAndQueueEvents(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)
	name := q.Name()

	b64 := base64.StdEncoding.EncodeToString([]byte(name))
	if err := c.Do(ctx, "CLIENT", "SETNAME", "bull:"+b64).Err(); err != nil {
		t.Fatal(err)
	}
	workers, err := q.Workers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatalf("Workers = %d entries, want 1", len(workers))
	}
	if workers[0]["name"] != "bull:"+b64 {
		t.Fatalf("worker name = %q", workers[0]["name"])
	}

	if err := c.Do(ctx, "CLIENT", "SETNAME", "bull:"+b64+":qe").Err(); err != nil {
		t.Fatal(err)
	}
	events, err := q.EventListeners(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("EventListeners = %d entries, want 1", len(events))
	}
}

// Manual processing: NextJob + Job.MoveToCompleted / MoveToFailed.
func TestManualProcessing(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "a", map[string]any{"i": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "b", map[string]any{"i": 2}); err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	job, err := w.NextJob(ctx, "manual-token-1")
	if err != nil || job == nil {
		t.Fatalf("NextJob: %v %v", job, err)
	}
	// fetchNext=true must hand back the prefetched job (locked with the same
	// token) instead of stranding it in active.
	job2, err := job.MoveToCompleted(ctx, "done", true)
	if err != nil {
		t.Fatalf("MoveToCompleted: %v", err)
	}
	if err := c.ZScore(ctx, prefix+"completed", job.ID()).Err(); err != nil {
		t.Fatalf("job not completed: %v", err)
	}
	if job2 == nil {
		t.Fatal("MoveToCompleted(fetchNext) did not return the next job")
	}

	if err := job2.MoveToFailed(ctx, errors.New("nope"), false); err != nil {
		t.Fatalf("MoveToFailed: %v", err)
	}
	if err := c.ZScore(ctx, prefix+"failed", job2.ID()).Err(); err != nil {
		t.Fatalf("job not failed: %v", err)
	}
	reason, _ := c.HGet(ctx, prefix+job2.ID(), "failedReason").Result()
	if reason != "nope" {
		t.Fatalf("failedReason = %q", reason)
	}
	// saveStacktrace parity: the stacktrace hash field must be written.
	st, _ := c.HGet(ctx, prefix+job2.ID(), "stacktrace").Result()
	if !strings.Contains(st, "nope") {
		t.Fatalf("stacktrace = %q, want the error recorded", st)
	}
}

// Convenience surface: UpdateJobProgress, JobCountByStates,
// RemoveDeprecatedPriorityKey, IsWaitingChildren, DependenciesCount, ClearLogs(keep).
func TestConvenienceSurface(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	job, err := q.Add(ctx, "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	if err := q.UpdateJobProgress(ctx, job.ID(), map[string]any{"pct": 50}); err != nil {
		t.Fatal(err)
	}
	var progress map[string]any
	raw, _ := c.HGet(ctx, prefix+job.ID(), "progress").Result()
	if err := json.Unmarshal([]byte(raw), &progress); err != nil || progress["pct"] != float64(50) {
		t.Fatalf("progress = %q (%v)", raw, err)
	}

	n, err := q.JobCountByStates(ctx, JobStateWaiting, JobStateDelayed)
	if err != nil || n != 1 {
		t.Fatalf("JobCountByStates = %d (%v), want 1", n, err)
	}

	// Counting 'waiting' includes 'paused' like upstream sanitizeJobTypes:
	// pause the queue, add another job (it lands on the paused list), and the
	// waiting count must cover both.
	if err := q.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "test2", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	n, err = q.JobCountByStates(ctx, JobStateWaiting)
	if err != nil || n != 2 {
		t.Fatalf("JobCountByStates(waiting) on paused queue = %d (%v), want 2", n, err)
	}
	if err := q.Resume(ctx); err != nil {
		t.Fatal(err)
	}

	if err := c.ZAdd(ctx, prefix+"priority", redis.Z{Score: 1, Member: "x"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.RemoveDeprecatedPriorityKey(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.Exists(ctx, prefix+"priority").Result(); n != 0 {
		t.Fatal("deprecated priority key not removed")
	}

	isWC, err := job.IsWaitingChildren(ctx)
	if err != nil || isWC {
		t.Fatalf("IsWaitingChildren = %v (%v), want false", isWC, err)
	}

	p, u, err := job.DependenciesCount(ctx)
	if err != nil || p != 0 || u != 0 {
		t.Fatalf("DependenciesCount = %d/%d (%v)", p, u, err)
	}

	for _, m := range []string{"one", "two", "three"} {
		if _, err := job.Log(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := job.ClearLogs(ctx, 2); err != nil {
		t.Fatal(err)
	}
	logs, _, err := job.Logs(ctx, 0, -1)
	if err != nil || len(logs) != 2 || logs[0] != "two" {
		t.Fatalf("logs after ClearLogs(2) = %v (%v)", logs, err)
	}
	if err := job.ClearLogs(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if logs, _, _ = job.Logs(ctx, 0, -1); len(logs) != 0 {
		t.Fatalf("logs after ClearLogs(0) = %v", logs)
	}
}
