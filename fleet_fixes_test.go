package gobullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
)

type blockingJSONResult struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r blockingJSONResult) MarshalJSON() ([]byte, error) {
	r.started <- struct{}{}
	<-r.release
	return []byte(`"ok"`), nil
}

func TestLegacyJobOptionsAndProgressHydration(t *testing.T) {
	opts, err := jobOptsFromJson(`{"attempts":1,"failParentOnFailure":true,"removeDependencyOnFailure":true,"repeat":{"every":60000,"prevMillis":1234}}`)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.FailParentOnFailure || !opts.RemoveDependencyOnFailure || opts.PrevMillis != 1234 {
		t.Fatalf("legacy options not normalized: %+v", opts)
	}
	wire, err := json.Marshal(opts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "failParentOnFailure") || strings.Contains(string(wire), "removeDependencyOnFailure") {
		t.Fatalf("legacy option names leaked back onto the wire: %s", wire)
	}
	if !strings.Contains(string(wire), `"fpof":true`) || !strings.Contains(string(wire), `"rdof":true`) || !strings.Contains(string(wire), `"prevMillis":1234`) {
		t.Fatalf("normalized option keys missing from %s", wire)
	}

	raw, err := jobFromJson(map[string]any{
		"id":       "1",
		"name":     "progress",
		"data":     `{}`,
		"opts":     `{}`,
		"progress": `{"pct":50,"phase":"work"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := wrapRawJob[map[string]any](&raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := job.Progress(), map[string]any{"pct": float64(50), "phase": "work"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Progress() = %#v, want %#v", got, want)
	}
}

func TestAddBulkMergesDefaultsAndRejectsPartialParentBatch(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"
	q, err := NewQueue[map[string]any](name, c, &QueueOptions{DefaultJobOptions: &JobOptions{
		Attempts: 5,
		Backoff:  &BackoffOptions{Type: "fixed", Delay: 25},
	}})
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := q.AddBulk(ctx, []BulkJob[map[string]any]{
		{Name: "merged", Data: map[string]any{}, Opts: JobOptions{Priority: 3}},
		{Name: "explicit-zero", Data: map[string]any{}, Options: []AddOption{AddWithAttempts(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].Options().Attempts != 5 || jobs[0].Options().Backoff == nil || jobs[0].Options().Priority != 3 {
		t.Fatalf("bulk defaults not merged: %+v", jobs[0].Options())
	}
	if jobs[1].Options().Attempts != 0 {
		t.Fatalf("presence-aware bulk override did not clear attempts: %+v", jobs[1].Options())
	}

	before, _ := c.LLen(ctx, prefix+"wait").Result()
	_, err = q.AddBulk(ctx, []BulkJob[map[string]any]{
		{Name: "valid", Data: map[string]any{}, Opts: JobOptions{JobID: "bulk-valid"}},
		{Name: "missing-parent", Data: map[string]any{}, Opts: JobOptions{
			JobID:  "bulk-child",
			Parent: &ParentOptions{ID: "missing", Queue: strings.TrimSuffix(prefix, ":")},
		}},
	})
	if err == nil {
		t.Fatal("AddBulk accepted a missing parent")
	}
	after, _ := c.LLen(ctx, prefix+"wait").Result()
	if after != before {
		t.Fatalf("missing-parent batch partially committed: wait %d -> %d", before, after)
	}
	if exists, _ := c.Exists(ctx, prefix+"bulk-valid", prefix+"bulk-child").Result(); exists != 0 {
		t.Fatalf("missing-parent batch created %d job hashes", exists)
	}
}

func TestWaitingCountsProgressAndChildrenValues(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)
	if err := q.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := q.Add(ctx, "paused", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	counts, err := q.JobCounts(ctx, JobStateWaiting)
	if err != nil {
		t.Fatal(err)
	}
	if counts[JobStateWaiting] != 1 {
		t.Fatalf("waiting count = %d, want 1 (all counts: %v)", counts[JobStateWaiting], counts)
	}
	if _, leaked := counts[JobStatePaused]; leaked {
		t.Fatalf("implicit paused state leaked into result: %v", counts)
	}

	if err := q.UpdateJobProgress(ctx, job.ID(), map[string]any{"pct": 75}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := q.Job(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := reloaded.Progress(), map[string]any{"pct": float64(75)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded progress = %#v, want %#v", got, want)
	}

	if err := c.HSet(ctx, prefix+job.ID()+":processed",
		"child:object", `{"ok":true}`,
		"child:number", `42`,
	).Err(); err != nil {
		t.Fatal(err)
	}
	queueValues, err := q.ChildrenValues(ctx, job.ID())
	if err != nil {
		t.Fatal(err)
	}
	jobValues, err := reloaded.ChildrenValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantValues := map[string]any{"child:object": map[string]any{"ok": true}, "child:number": float64(42)}
	if !reflect.DeepEqual(queueValues, wantValues) || !reflect.DeepEqual(jobValues, wantValues) {
		t.Fatalf("decoded child values = queue %#v job %#v, want %#v", queueValues, jobValues, wantValues)
	}
}

func TestMoveToFailedFetchNextDoesNotStrandNextJob(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)
	first, err := q.Add(ctx, "first", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Add(ctx, "second", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker[map[string]any, string](q.Name(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx
	fetched, err := w.NextJob(ctx, "manual-token")
	if err != nil || fetched == nil || fetched.ID() != first.ID() {
		t.Fatalf("NextJob = %v, %v; want %s", fetched, err, first.ID())
	}
	if err := fetched.MoveToFailed(ctx, errors.New("boom"), true); err != nil {
		t.Fatal(err)
	}
	if state, _ := q.JobState(ctx, second.ID()); state != JobStateWaiting {
		t.Fatalf("next job state = %s, want waiting", state)
	}
}

func TestShutdownWaitsThroughRedisSettlement(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, _ := testQueue(t, c)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	w, err := NewWorker[map[string]any, blockingJSONResult](q.Name(), c,
		func(context.Context, *Job[map[string]any]) (blockingJSONResult, error) {
			return blockingJSONResult{started: started, release: release}, nil
		}, &WorkerOptions{Concurrency: 1, DrainDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = w.Close()
	})
	job, err := q.Add(ctx, "settlement", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("processor result never reached JSON settlement")
	}

	shutdownDone := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() { shutdownDone <- w.Shutdown(shutdownCtx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before settlement completed: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if state, _ := q.JobState(ctx, job.ID()); state != JobStateCompleted {
		t.Fatalf("settled job state = %s, want completed", state)
	}
}

func TestShutdownKeepsRenewingLocksWhileDraining(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	w, err := NewWorker[map[string]any, string](q.Name(), c,
		func(context.Context, *Job[map[string]any]) (string, error) {
			started <- struct{}{}
			<-release
			return "ok", nil
		}, &WorkerOptions{Concurrency: 1, LockDuration: 150 * time.Millisecond, LockRenewTime: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = w.Close()
	})
	job, err := q.Add(ctx, "long", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("processor did not start")
	}
	shutdownDone := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go func() { shutdownDone <- w.Shutdown(shutdownCtx) }()
	time.Sleep(350 * time.Millisecond)
	if exists, _ := c.Exists(ctx, prefix+job.ID()+":lock").Result(); exists != 1 {
		t.Fatal("job lock expired while graceful shutdown was draining")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestRepeatRemovalIsAtomicAndDeletesLegacyInstances(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	repeatKey := repeat.GetKey("legacy", repeat.RepeatKeyOpts{Every: 60_000})
	millis := time.Now().Add(time.Minute).UnixMilli()
	namespace := utils.MD5Hash(repeatKey)
	legacyID := repeat.GetLegacyJobId("legacy", strconv.FormatInt(millis, 10), namespace, "")
	if err := c.ZAdd(ctx, prefix+"repeat", redis.Z{Score: float64(millis), Member: repeatKey}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.ZAdd(ctx, prefix+"delayed", redis.Z{Score: float64(millis * 0x1000), Member: legacyID}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, prefix+legacyID, "name", "legacy").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.RemoveRepeatableByKey(ctx, repeatKey); err != nil {
		t.Fatal(err)
	}
	if count, _ := c.ZCard(ctx, prefix+"repeat").Result(); count != 0 {
		t.Fatalf("legacy repeat registry still has %d entries", count)
	}
	if count, _ := c.ZCard(ctx, prefix+"delayed").Result(); count != 0 {
		t.Fatalf("legacy delayed set still has %d entries", count)
	}
	if exists, _ := c.Exists(ctx, prefix+legacyID).Result(); exists != 0 {
		t.Fatal("legacy delayed job hash was not deleted")
	}

	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("race-%d", i)
		first, err := q.Add(ctx, name, map[string]any{}, AddWithRepeat(JobRepeatOptions{Every: 60_000}))
		if err != nil {
			t.Fatal(err)
		}
		opts := first.Options()
		key := first.RepeatJobKey()
		// Workers schedule the next iteration only after the current one has
		// left delayed and moved to active. Remove it here to model that state
		// without invoking the worker path, which would schedule immediately.
		removed, err := c.ZRem(ctx, prefix+"delayed", first.ID()).Result()
		if err != nil || removed != 1 {
			t.Fatalf("prepare repeat race %d: removed=%d err=%v", i, removed, err)
		}
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			_, scheduleErr := addNextRepeatableJobInternal(ctx, c, prefix, name, `{}`, opts, false)
			errs <- scheduleErr
		}()
		go func() {
			<-start
			errs <- q.RemoveRepeatableByKey(ctx, key)
		}()
		close(start)
		for n := 0; n < 2; n++ {
			if err := <-errs; err != nil {
				t.Fatalf("repeat race %d: %v", i, err)
			}
		}
		if repeatCount, _ := c.ZCard(ctx, prefix+"repeat").Result(); repeatCount != 0 {
			repeatEntries, _ := c.ZRangeWithScores(ctx, prefix+"repeat", 0, -1).Result()
			t.Fatalf("repeat race %d recreated the registry: %v", i, repeatEntries)
		}
		if delayedCount, _ := c.ZCard(ctx, prefix+"delayed").Result(); delayedCount != 0 {
			delayedEntries, _ := c.ZRangeWithScores(ctx, prefix+"delayed", 0, -1).Result()
			t.Fatalf("repeat race %d left delayed jobs: %v", i, delayedEntries)
		}
	}
	if count, _ := c.ZCard(ctx, prefix+"repeat").Result(); count != 0 {
		t.Fatalf("repeat removal race recreated %d registry entries", count)
	}
	if count, _ := c.ZCard(ctx, prefix+"delayed").Result(); count != 0 {
		remaining, _ := c.ZRange(ctx, prefix+"delayed", 0, -1).Result()
		t.Fatalf("repeat removal race left %d delayed jobs: %v", count, remaining)
	}
}

func TestTerminalStalledChildAppliesParentPolicies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		childOpts  JobOptions
		parentWant JobState
	}{
		{name: "fail-parent", childOpts: JobOptions{FailParentOnFailure: true}, parentWant: JobStateFailed},
		{name: "remove-dependency", childOpts: JobOptions{RemoveDependencyOnFailure: true}, parentWant: JobStateWaiting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := testClient(t)
			name := testQueueName(t, c)
			prefix := "bull:" + name + ":"
			producer, err := NewFlowProducer(c, nil)
			if err != nil {
				t.Fatal(err)
			}
			tree, err := producer.Add(ctx, FlowJob{
				Name: "parent", QueueName: name, Data: map[string]any{},
				Children: []FlowJob{{Name: "child", QueueName: name, Data: map[string]any{}, Opts: tc.childOpts}},
			})
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWorker[map[string]any, string](name, c, nil, &WorkerOptions{MaxStalledCount: -1})
			if err != nil {
				t.Fatal(err)
			}
			w.runCtx = ctx
			child, err := w.moveToActive(ctx, "stalled-token", "")
			if err != nil || child == nil || child.id != tree.Children[0].Job.ID() {
				t.Fatalf("moveToActive child = %v, %v", child, err)
			}
			if err := c.Del(ctx, prefix+child.id+":lock").Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.SAdd(ctx, prefix+"stalled", child.id).Err(); err != nil {
				t.Fatal(err)
			}
			if err := w.moveStalledJobsToWait(); err != nil {
				t.Fatal(err)
			}
			state, err := tree.Job.State(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if state != tc.parentWant {
				t.Fatalf("parent state = %s, want %s", state, tc.parentWant)
			}
		})
	}
}
