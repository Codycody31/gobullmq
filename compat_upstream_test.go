package gobullmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// The blocking fetch must be BRPOPLPUSH-equivalent: pop the OLDEST job from
// the right of wait (addJob LPUSHes), push to the left of active.
func TestBlockingFetchIsFIFO(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := q.Add(ctx, "a", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "b", map[string]any{}); err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.drained.Store(true) // force the blocking BLMove path
	got, err := w.getNextJob(ctx, w.token.String()+":1", nextJobOptions{Block: true})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.id != first.ID() {
		t.Fatalf("blocking fetch returned %+v, want oldest job %s (FIFO)", got, first.ID())
	}
}

// A BLMove block timeout is not an error: the worker must fall through to
// moveToActive, which promotes due delayed jobs (the only mechanism by which
// an idle worker picks up delayed work in v4).
func TestBlockTimeoutPromotesDelayedJobs(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "soon", map[string]any{}, AddWithDelay(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, &WorkerOptions{DrainDelay: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.drained.Store(true)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("idle blocking worker never promoted the delayed job")
		}
		got, err := w.getNextJob(ctx, w.token.String()+":1", nextJobOptions{Block: true})
		if err != nil {
			t.Fatalf("block timeout surfaced as error: %v", err)
		}
		if got != nil {
			if got.id != job.ID() {
				t.Fatalf("got %s, want %s", got.id, job.ID())
			}
			return
		}
	}
}

// Age-only retention ({age} with no count) must round-trip without gaining a
// count:0 (which the Lua reads as "remove immediately").
func TestKeepJobsAgeOnlySerialization(t *testing.T) {
	b, _ := json.Marshal(KeepJobs{Age: 3600})
	if string(b) != `{"age":3600}` {
		t.Fatalf("age-only KeepJobs = %s, want {\"age\":3600}", b)
	}
	b, _ = json.Marshal(KeepJobs{})
	if string(b) != `{"count":0}` {
		t.Fatalf("zero KeepJobs = %s, want {\"count\":0}", b)
	}
	b, _ = json.Marshal(KeepJobs{Count: -1})
	if string(b) != `{"count":-1}` {
		t.Fatalf("keep-all KeepJobs = %s, want {\"count\":-1}", b)
	}
	b, _ = json.Marshal(KeepJobs{Age: 60, Count: 5})
	if string(b) != `{"age":60,"count":5}` {
		t.Fatalf("age+count KeepJobs = %s", b)
	}

	// End to end: a job completed with age-only retention must survive in the
	// completed zset (count:0 would delete it immediately).
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"
	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "age-only", map[string]any{}, AddWithRemoveOnComplete(KeepJobs{Age: 3600}))
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
	if _, err := w.handleJobSuccess(*fetched, token, "ok", func() bool { return false }); err != nil {
		t.Fatal(err)
	}
	if err := c.ZScore(ctx, prefix+"completed", job.ID()).Err(); err != nil {
		t.Fatalf("age-only job was removed on completion: %v", err)
	}
}

// Stored opts must not contain fields upstream never writes: timestamp (unless
// caller-supplied), repeatJobKey, token.
func TestStoredOptsHaveNoExtraFields(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	job, err := q.Add(ctx, "plain", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, _ := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	var opts map[string]any
	_ = json.Unmarshal([]byte(optsJSON), &opts)
	if _, has := opts["timestamp"]; has {
		t.Fatalf("plain add stored opts.timestamp: %s", optsJSON)
	}

	tj, err := q.Add(ctx, "with-ts", map[string]any{}, AddWithTimestamp(1234567890123))
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, _ = c.HGet(ctx, prefix+tj.ID(), "opts").Result()
	if !strings.Contains(optsJSON, `"timestamp":1234567890123`) {
		t.Fatalf("caller-supplied timestamp missing from opts: %s", optsJSON)
	}

	rep, err := q.Add(ctx, "rep", map[string]any{}, AddWithRepeat(JobRepeatOptions{Every: 60_000}))
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, _ = c.HGet(ctx, prefix+rep.ID(), "opts").Result()
	if strings.Contains(optsJSON, "repeatJobKey") {
		t.Fatalf("repeat iteration stored opts.repeatJobKey (upstream strips it): %s", optsJSON)
	}
	// The rjk hash field itself must still be present.
	if rjk, _ := c.HGet(ctx, prefix+rep.ID(), "rjk").Result(); rjk == "" {
		t.Fatal("rjk hash field missing")
	}
}

// Job names are stored as passed; there is no __default__ substitution in
// BullMQ v4 (that was Bull v3).
func TestEmptyJobNameStoredVerbatim(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)
	job, err := q.Add(ctx, "", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	name, err := c.HGet(ctx, prefix+job.ID(), "name").Result()
	if err != nil {
		t.Fatal(err)
	}
	if name != "" {
		t.Fatalf("empty name stored as %q, want empty", name)
	}
}

// AddBulk must store the full opts (kl etc.) and be all-or-nothing.
func TestAddBulkAtomicAndFullOpts(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	jobs, err := q.AddBulk(ctx, []BulkJob[map[string]any]{
		{Name: "a", Data: map[string]any{}, Opts: JobOptions{KeepLogs: 4, StackTraceLimit: 2}},
		{Name: "b", Data: map[string]any{}, Opts: JobOptions{Priority: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("AddBulk returned %d jobs", len(jobs))
	}
	optsJSON, _ := c.HGet(ctx, prefix+jobs[0].ID(), "opts").Result()
	if !strings.Contains(optsJSON, `"kl":4`) || !strings.Contains(optsJSON, `"stackTraceLimit":2`) {
		t.Fatalf("AddBulk dropped opts: %s", optsJSON)
	}

	// A bad custom id anywhere fails the whole batch before anything lands.
	before, _ := c.LLen(ctx, prefix+"wait").Result()
	_, err = q.AddBulk(ctx, []BulkJob[map[string]any]{
		{Name: "ok", Data: map[string]any{}},
		{Name: "bad", Data: map[string]any{}, Opts: JobOptions{JobID: "0:99"}},
	})
	if err == nil {
		t.Fatal("bad jobId in bulk accepted")
	}
	after, _ := c.LLen(ctx, prefix+"wait").Result()
	if after != before {
		t.Fatalf("partial bulk added: wait %d -> %d", before, after)
	}
}

// PromoteJobs must batch through moveJobsToWait and emit 'waiting' events
// like upstream, not per-job 'promoted' events.
func TestPromoteJobsBatchesLikeUpstream(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	for i := 0; i < 3; i++ {
		if _, err := q.Add(ctx, "d", map[string]any{"i": i}, AddWithDelay(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.PromoteJobs(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.ZCard(ctx, prefix+"delayed").Result(); n != 0 {
		t.Fatalf("%d jobs left in delayed", n)
	}
	entries, _ := c.LRange(ctx, prefix+"wait", 0, -1).Result()
	jobsInWait := 0
	for _, e := range entries {
		if !strings.HasPrefix(e, "0:") { // skip delayed markers
			jobsInWait++
		}
	}
	if jobsInWait != 3 {
		t.Fatalf("wait has %d jobs (%v), want 3", jobsInWait, entries)
	}
	msgs, _ := c.XRange(ctx, prefix+"events", "-", "+").Result()
	waiting, promoted := 0, 0
	for _, m := range msgs {
		switch m.Values["event"] {
		case "waiting":
			waiting++
		case "promoted":
			promoted++
		}
	}
	if promoted != 0 {
		t.Fatalf("PromoteJobs emitted %d 'promoted' events; upstream emits 'waiting'", promoted)
	}
	if waiting < 3 {
		t.Fatalf("expected >=3 waiting events, got %d", waiting)
	}
}

// The queue writes meta opts.maxLenEvents on first use (upstream does it in
// the constructor); JS workers' trimEvents reads it.
func TestMetaMaxLenEventsWrittenOnFirstAdd(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)
	if _, err := q.Add(ctx, "x", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	v, err := c.HGet(ctx, prefix+"meta", "opts.maxLenEvents").Result()
	if err != nil || v != "10000" {
		t.Fatalf("meta opts.maxLenEvents = %q (%v), want 10000", v, err)
	}
}

// The worker fail path must not prefetch (upstream moveToFailed always sends
// fetchNext=0): failing a job must not strand the next waiting job in active.
func TestFailPathDoesNotStrandNextJob(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "one", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	second, err := q.Add(ctx, "two", map[string]any{})
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
	// fetchNextCallback returns true; the fail path must ignore it.
	w.handleJobError(*fetched, token, errors.New("boom"))

	if n, _ := c.LLen(ctx, prefix+"active").Result(); n != 0 {
		ids, _ := c.LRange(ctx, prefix+"active", 0, -1).Result()
		t.Fatalf("fail path stranded jobs in active: %v", ids)
	}
	state, _ := q.JobState(ctx, second.ID())
	if state != "waiting" {
		t.Fatalf("second job state = %s, want waiting", state)
	}
}

// The Run loop must chain jobs returned by moveToFinished's fetch-next
// (upstream processes them under the same token instead of orphaning them).
func TestRunLoopChainsPrefetchedJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testClient(t)
	name := testQueueName(t, c)

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		j, err := q.Add(ctx, "n", map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID())
	}

	w, err := NewWorker[map[string]any, string](name, c,
		func(ctx context.Context, job *Job[map[string]any]) (string, error) { return "ok", nil },
		&WorkerOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			counts, _ := q.JobCounts(ctx, JobStateCompleted, JobStateActive, JobStateWaiting)
			t.Fatalf("jobs not all completed: %v", counts)
		}
		counts, err := q.JobCounts(ctx, JobStateCompleted)
		if err == nil && counts[JobStateCompleted] == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = ids
}

// Repeatable adds accept custom jobId '0' (the guard applies only to plain
// adds upstream), and a repeat object without every/pattern is rejected.
func TestRepeatGuardOrdering(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)

	if _, err := q.Add(ctx, "r", map[string]any{}, AddWithRepeat(JobRepeatOptions{Every: 60_000}), AddWithJobID("0")); err != nil {
		t.Fatalf("repeatable add with jobId '0' rejected: %v", err)
	}
	if _, err := q.Add(ctx, "bad", map[string]any{}, AddWithRepeat(JobRepeatOptions{Limit: 5})); err == nil {
		t.Fatal("repeat opts without every/pattern accepted; upstream rejects via cron-parser throw")
	}
}
