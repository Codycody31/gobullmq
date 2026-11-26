package gobullmq

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
)

// These tests cover the idle worker. Issue #40 was a drained queue never
// parking the worker in its blocking read, and fixing that switched the read
// on, so pausing, closing, delayed jobs and rate limits are all checked here
// with the worker parked in it.

type idleData = map[string]any

// cmdCounter counts what an idle worker sends on one client: script calls,
// blocking reads, and the CLIENT ID and CLIENT UNBLOCK around a pinned read.
type cmdCounter struct {
	evalsha  atomic.Int64
	blmove   atomic.Int64
	clientID atomic.Int64
	unblock  atomic.Int64
	// readKeys is the set of wait-list keys a blocking read has gone out for.
	// It tells the workers apart when several share the client.
	readKeys sync.Map
}

// readSent reports whether a blocking read has gone out for the wait list at
// key.
func (h *cmdCounter) readSent(key string) bool {
	_, ok := h.readKeys.Load(key)
	return ok
}

// subcommand returns the lowercased second word of cmd ("unblock" for
// CLIENT UNBLOCK), or "".
func subcommand(cmd redis.Cmder) string {
	if args := cmd.Args(); len(args) > 1 {
		s, _ := args[1].(string)
		return strings.ToLower(s)
	}
	return ""
}

func (h *cmdCounter) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *cmdCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "evalsha", "eval":
			h.evalsha.Add(1)
		case "blmove":
			h.blmove.Add(1)
			if args := cmd.Args(); len(args) > 1 {
				h.readKeys.Store(args[1], struct{}{})
			}
		case "client":
			switch subcommand(cmd) {
			case "id":
				h.clientID.Add(1)
			case "unblock":
				h.unblock.Add(1)
			}
		}
		return next(ctx, cmd)
	}
}

func (h *cmdCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// replyError stands in for an error reply from the server.
type replyError string

func (e replyError) Error() string { return string(e) }
func (replyError) RedisError()     {}

// refuseCmd fails a CLIENT subcommand with err, as a server would that does
// not have it. It refuses the first limit calls, or every call when limit is
// zero.
type refuseCmd struct {
	subcommand string
	err        error
	limit      int64
	refused    atomic.Int64
}

func (h *refuseCmd) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *refuseCmd) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "client" && subcommand(cmd) == h.subcommand {
			if n := h.refused.Add(1); h.limit == 0 || n <= h.limit {
				return h.err
			}
		}
		return next(ctx, cmd)
	}
}

func (h *refuseCmd) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// readMode is one of the two ways an idle worker waits: the interruptible
// read on a pinned connection, or the one-second slices it falls back to.
type readMode struct {
	name   string
	sliced bool
}

var (
	pinnedMode = readMode{name: "pinned"}
	slicedMode = readMode{name: "sliced", sliced: true}
	readModes  = []readMode{pinnedMode, slicedMode}
)

// setup puts w in this mode before it runs.
func (m readMode) setup(w *Worker[idleData, string]) {
	if m.sliced {
		w.pinnedRead.Store(pinnedReadOff)
	}
}

// check fails the test unless w settles on waiting this way.
func (m readMode) check(t *testing.T, w *Worker[idleData, string]) {
	t.Helper()
	want := pinnedReadOn
	if m.sliced {
		want = pinnedReadOff
	}
	waitUntil(t, 2*time.Second, "the worker to wait in "+m.name+" mode", func() bool {
		return w.pinnedRead.Load() == want
	})
}

// waitUntil polls cond until it holds or timeout passes.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// errorLog collects a worker's error events.
type errorLog struct {
	mu   sync.Mutex
	errs []string
}

func (l *errorLog) add(err error) {
	l.mu.Lock()
	l.errs = append(l.errs, err.Error())
	l.mu.Unlock()
}

func (l *errorLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.errs...)
}

// runIdleWorker starts a worker on an empty queue and returns once it has
// drained and parked in the blocking read. Each setup runs on the worker
// before it starts. The caller closes it.
func runIdleWorker(t *testing.T, name string, c redis.Cmdable, process ProcessFunc[idleData, string], opts *WorkerOptions, setup ...func(*Worker[idleData, string])) (*Worker[idleData, string], *errorLog) {
	t.Helper()
	w, err := NewWorker[idleData, string](name, c, process, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range setup {
		fn(w)
	}
	errs := &errorLog{}
	w.OnError(errs.add)
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the worker to drain", w.drained.Load)
	waitUntil(t, 5*time.Second, "the blocking read to start", func() bool { return !w.blockingReadIdle() })
	return w, errs
}

func listOf(t *testing.T, c *redis.Client, key string) []string {
	t.Helper()
	ids, err := c.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// The script's "no job" reply is {0, 0, 0, delayUntil}: a Lua integer sits in
// the job-data slot. It must decode to "no job data", as upstream's falsy
// raw[0] does, not to an empty job.
func TestRaw2NextJobDataEmptyReply(t *testing.T) {
	got := raw2NextJobData([]any{int64(0), int64(0), int64(0), int64(1700000000000)})
	switch {
	case got == nil:
		t.Fatal("nil result for a well-formed empty reply")
	case got.JobData != nil:
		t.Fatalf("JobData = %#v, want nil for the empty reply", got.JobData)
	case got.DelayUntil != 1700000000000:
		t.Fatalf("DelayUntil = %d, want the next delayed timestamp", got.DelayUntil)
	}
}

// Delay markers carry a Lua number, which prints with a fraction for most job
// counters; upstream reads it with parseInt(...) || 0.
func TestMarkerTimestamp(t *testing.T) {
	for marker, want := range map[string]int64{
		"0:1795703941889":   1795703941889,
		"0:1795703941889.1": 1795703941889,
		"0:1795703941889.9": 1795703941889,
		"0:0":               0,
		"0:":                0,
		"0:soon":            0,
		"0:-5":              0,
		"0:NaN":             0,
		"0:1e400":           0,
	} {
		if got := markerTimestamp(marker); got != want {
			t.Errorf("markerTimestamp(%q) = %d, want %d", marker, got, want)
		}
	}
}

// Issue #40: an idle worker must mark itself drained and park in the blocking
// read instead of calling moveToActive in a tight loop.
func TestIdleWorkerBlocksInsteadOfPolling(t *testing.T) {
	c := testClient(t)
	name := testQueueName(t, c)
	counter := &cmdCounter{}
	c.AddHook(counter)

	var drained atomic.Int64
	w, err := NewWorker[idleData, string](name, c,
		func(ctx context.Context, job *Job[idleData]) (string, error) { return "ok", nil },
		&WorkerOptions{Concurrency: 4, DrainDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	w.OnDrained(func() { drained.Add(1) })
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	time.Sleep(2500 * time.Millisecond)

	if !w.drained.Load() {
		t.Error("idle worker is not in the drained state")
	}
	if n := drained.Load(); n != 1 {
		t.Errorf("drained emitted %d times, want once", n)
	}
	if n := counter.blmove.Load(); n == 0 {
		t.Error("idle worker never issued a blocking read")
	}
	// A handful is expected: the initial fetches, the stalled check, and one
	// moveToActive per elapsed DrainDelay. Before the fix it was over 100,000.
	if n := counter.evalsha.Load(); n > 50 {
		t.Errorf("idle worker issued %d script calls in 2.5s, want a handful", n)
	}
}

// A queue that has handed out a few hundred job IDs produces fractional delay
// markers. The blocking read must take one in its stride: no error, nothing
// left behind in active, and the delayed job fetched when it is due.
func TestBlockingFetchHandlesFractionalDelayMarker(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, prefix+"id", 300, 0).Err(); err != nil {
		t.Fatal(err)
	}
	const delay = 400 * time.Millisecond
	added := time.Now()
	job, err := q.Add(ctx, "later", idleData{}, AddWithDelay(delay))
	if err != nil {
		t.Fatal(err)
	}
	wait := listOf(t, c, prefix+"wait")
	if len(wait) != 1 || !strings.HasPrefix(wait[0], "0:") || !strings.Contains(wait[0], ".") {
		t.Fatalf("wait = %v, want one fractional delay marker (test precondition)", wait)
	}
	marker := wait[0]

	w, err := NewWorker[idleData, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.drained.Store(true) // force the blocking path
	token := w.token.String() + ":1"

	got, err := w.getNextJob(ctx, token, nextJobOptions{Block: true})
	if err != nil {
		t.Fatalf("fetching the delay marker failed: %v", err)
	}
	if got != nil {
		t.Fatalf("got job %s before its delay elapsed", got.id)
	}
	if active := listOf(t, c, prefix+"active"); len(active) != 0 {
		t.Fatalf("active = %v, want the marker removed", active)
	}
	if until := w.blockUntil.Load(); until != markerTimestamp(marker) {
		t.Fatalf("blockUntil = %d, want the marker's timestamp %d", until, markerTimestamp(marker))
	}

	for got == nil {
		if time.Since(added) > delay+2*time.Second {
			t.Fatal("delayed job was not fetched once due")
		}
		if got, err = w.getNextJob(ctx, token, nextJobOptions{Block: true}); err != nil {
			t.Fatal(err)
		}
	}
	if got.id != job.ID() {
		t.Fatalf("got %s, want %s", got.id, job.ID())
	}
	if early := delay - time.Since(added); early > 20*time.Millisecond {
		t.Fatalf("delayed job fetched %s early", early)
	}
}

// A delay shorter than a second must not be rounded up to one, and the
// worker must stay responsive to other jobs meanwhile.
func TestIdleWorkerRunsSubSecondDelayOnTime(t *testing.T) {
	for _, mode := range readModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			c := testClient(t)
			name := testQueueName(t, c)
			q, err := NewQueue[idleData](name, c, nil)
			if err != nil {
				t.Fatal(err)
			}
			// Start past the low job counters, whose markers happen to print
			// as integers.
			if err := c.Set(ctx, "bull:"+name+":id", 300, 0).Err(); err != nil {
				t.Fatal(err)
			}

			ran := make(chan string, 2)
			w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
				ran <- job.Name()
				return "ok", nil
			}, nil, mode.setup)
			defer w.Close()

			const delay = 250 * time.Millisecond
			added := time.Now()
			if _, err := q.Add(ctx, "delayed", idleData{}, AddWithDelay(delay)); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Add(ctx, "standard", idleData{}); err != nil {
				t.Fatal(err)
			}

			for _, want := range []string{"standard", "delayed"} {
				select {
				case got := <-ran:
					if got != want {
						t.Fatalf("ran %q, want %q first", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("%s job did not run (worker errors: %v)", want, errs.all())
				}
			}
			// The promotion script moves a job up to a millisecond before it is due.
			if took := time.Since(added); took < delay-10*time.Millisecond || took > delay+500*time.Millisecond {
				t.Fatalf("job delayed %s ran after %s", delay, took)
			}
			if e := errs.all(); len(e) != 0 {
				t.Fatalf("worker errors: %v", e)
			}
			mode.check(t, w)
		})
	}
}

// With a connection pinned, the worker sleeps through the wait for a delayed
// job in one read that is cut off when the job is due. It does not poll.
func TestPinnedReadWakesForDelayedJobWithoutPolling(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	counter := &cmdCounter{}
	c.AddHook(counter)
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "bull:"+name+":id", 300, 0).Err(); err != nil {
		t.Fatal(err)
	}

	ran := make(chan time.Time, 1)
	w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		ran <- time.Now()
		return "ok", nil
	}, nil)
	defer w.Close()

	// 1.3s is not a whole number of seconds, and the sliced wait could only
	// cover its last 300ms by polling.
	const delay = 1300 * time.Millisecond
	scriptsBefore, readsBefore := counter.evalsha.Load(), counter.blmove.Load()
	added := time.Now()
	if _, err := q.Add(ctx, "delayed", idleData{}, AddWithDelay(delay)); err != nil {
		t.Fatal(err)
	}
	var at time.Time
	select {
	case at = <-ran:
	case <-time.After(5 * time.Second):
		t.Fatalf("delayed job did not run (worker errors: %v)", errs.all())
	}
	if took := at.Sub(added); took < delay-10*time.Millisecond || took > delay+300*time.Millisecond {
		t.Fatalf("job delayed %s ran after %s", delay, took)
	}
	// addJob, the moveToActive that takes the marker and the one that
	// promotes the job, plus moveToFinished if the job is already done.
	if n := counter.evalsha.Load() - scriptsBefore; n > 4 {
		t.Errorf("%d script calls while waiting for the delayed job, want at most 4", n)
	}
	// One read waits for the job to come due. The next idle read may have
	// started by now.
	if n := counter.blmove.Load() - readsBefore; n > 2 {
		t.Errorf("%d blocking reads while waiting for the delayed job, want at most 2", n)
	}
	if n := counter.unblock.Load(); n == 0 {
		t.Error("the read was never cut off with CLIENT UNBLOCK")
	}
	pinnedMode.check(t, w)
}

// DrainDelay is honoured to the millisecond on a pinned connection; the
// sliced wait can only round it up to a second.
func TestSubSecondDrainDelay(t *testing.T) {
	for _, mode := range readModes {
		t.Run(mode.name, func(t *testing.T) {
			c := testClient(t)
			name := testQueueName(t, c)
			counter := &cmdCounter{}
			c.AddHook(counter)

			w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
				return "ok", nil
			}, &WorkerOptions{DrainDelay: 100 * time.Millisecond}, mode.setup)
			defer w.Close()
			mode.check(t, w)

			before := counter.blmove.Load()
			time.Sleep(time.Second)
			reads := counter.blmove.Load() - before

			// Ten waits of 100ms fit in a second, or one of a whole second.
			lo, hi := int64(6), int64(11)
			if mode.sliced {
				lo, hi = 0, 2
			}
			if reads < lo || reads > hi {
				t.Fatalf("%d blocking reads in 1s with DrainDelay 100ms, want %d to %d", reads, lo, hi)
			}
			if e := errs.all(); len(e) != 0 {
				t.Fatalf("worker errors: %v", e)
			}
		})
	}
}

// An idle worker costs one CLIENT ID, one BLMOVE and one script call per
// DrainDelay. The read times out on its own when the wait is a whole number
// of seconds, so nothing has to unblock it.
func TestPinnedReadIdleCommandBudget(t *testing.T) {
	c := testClient(t)
	name := testQueueName(t, c)
	counter := &cmdCounter{}
	c.AddHook(counter)

	w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		return "ok", nil
	}, &WorkerOptions{DrainDelay: time.Second, SkipStalledCheck: true})
	defer w.Close()
	pinnedMode.check(t, w)

	ids, reads, scripts := counter.clientID.Load(), counter.blmove.Load(), counter.evalsha.Load()
	time.Sleep(2500 * time.Millisecond)
	ids, reads, scripts = counter.clientID.Load()-ids, counter.blmove.Load()-reads, counter.evalsha.Load()-scripts

	for what, n := range map[string]int64{"CLIENT ID": ids, "BLMOVE": reads, "script": scripts} {
		if n < 2 || n > 3 {
			t.Errorf("%d %s calls in 2.5s with DrainDelay 1s, want 2 or 3", n, what)
		}
	}
	// The one CLIENT UNBLOCK is the check that the server has the command.
	if n := counter.unblock.Load(); n != 1 {
		t.Errorf("%d CLIENT UNBLOCK calls, want 1", n)
	}
	if e := errs.all(); len(e) != 0 {
		t.Fatalf("worker errors: %v", e)
	}
}

// After a rate-limit window the worker must ask moveToActive directly.
// Prioritized jobs are not in the wait list, so a blocking read there would
// sit out a full DrainDelay per window.
func TestRateLimitedPrioritizedJobsSkipTheBlockingRead(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}

	const jobs = 3
	ran := make(chan struct{}, jobs)
	w, _ := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		ran <- struct{}{}
		return "ok", nil
	}, &WorkerOptions{Limiter: &RateLimiterOptions{Max: 1, Duration: 300}}) // DrainDelay stays 5s
	defer w.Close()

	start := time.Now()
	for i := 0; i < jobs; i++ {
		if _, err := q.Add(ctx, "p", idleData{}, AddWithPriority(3)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < jobs; i++ {
		select {
		case <-ran:
		case <-time.After(3 * time.Second):
			t.Fatalf("job %d of %d did not start within %s; want about 600ms in total", i+1, jobs, time.Since(start))
		}
	}
}

// One "0:0" marker announces any number of prioritized jobs. Fetchers queued
// behind the one it woke must go for the rest instead of blocking again.
func TestPrioritizedBurstUsesEverySlot(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}

	const concurrency, jobs = 3, 6
	var mu sync.Mutex
	running, peak, finished := 0, 0, 0
	done := make(chan struct{})
	w, _ := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		mu.Lock()
		running++
		if running > peak {
			peak = running
		}
		mu.Unlock()
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		running--
		finished++
		if finished == jobs {
			close(done)
		}
		mu.Unlock()
		return "ok", nil
	}, &WorkerOptions{Concurrency: concurrency})
	defer w.Close()

	// Queue the burst while the queue is paused: on resume the jobs become
	// available at once behind a single marker, as with a bulk add.
	if err := q.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < jobs; i++ {
		if _, err := q.Add(ctx, "p", idleData{}, AddWithPriority(2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Resume(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("burst did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if peak != concurrency {
		t.Fatalf("at most %d jobs ran at once, want %d", peak, concurrency)
	}
}

// A job that arrives after Pause must stay in the queue. On a pinned
// connection Pause ends the blocking read; the sliced read cannot be
// interrupted, so a job it pulls goes back unprocessed.
func TestPauseOnIdleWorkerLeavesNewJobWaiting(t *testing.T) {
	for _, mode := range readModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			c := testClient(t)
			name := testQueueName(t, c)
			q, err := NewQueue[idleData](name, c, nil)
			if err != nil {
				t.Fatal(err)
			}

			var processed atomic.Int64
			w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
				processed.Add(1)
				return "ok", nil
			}, nil, mode.setup)
			defer w.Close()

			// Long enough for the read in flight to have ended either way.
			settle := 300 * time.Millisecond
			w.Pause()
			if mode.sliced {
				settle += blockingReadSlice
			} else {
				waitUntil(t, 500*time.Millisecond, "Pause to end the blocking read", w.blockingReadIdle)
			}
			job, err := q.Add(ctx, "after-pause", idleData{})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(settle)

			if n := processed.Load(); n != 0 {
				t.Fatalf("paused worker processed %d job(s)", n)
			}
			if s, _ := q.JobState(ctx, job.ID()); s != JobStateWaiting {
				t.Fatalf("job state while paused = %s, want waiting", s)
			}
			if e := errs.all(); len(e) != 0 {
				t.Fatalf("worker errors: %v", e)
			}

			w.Resume()
			waitUntil(t, 5*time.Second, "the resumed worker to process the job", func() bool { return processed.Load() == 1 })
			mode.check(t, w)
		})
	}
}

// PauseAndWait returns only once the blocking read has ended, so from then on
// a new job never even passes through active.
func TestPauseAndWaitCoversTheBlockingRead(t *testing.T) {
	for _, mode := range readModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			c := testClient(t)
			name := testQueueName(t, c)
			q, err := NewQueue[idleData](name, c, nil)
			if err != nil {
				t.Fatal(err)
			}

			w, _ := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
				return "ok", nil
			}, nil, mode.setup)
			defer w.Close()

			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			start := time.Now()
			if err := w.PauseAndWait(waitCtx); err != nil {
				t.Fatalf("PauseAndWait: %v", err)
			}
			if took := time.Since(start); !mode.sliced && took > 500*time.Millisecond {
				t.Fatalf("PauseAndWait took %s on an idle worker", took)
			}
			if !w.blockingReadIdle() {
				t.Fatal("PauseAndWait returned with the blocking read still outstanding")
			}

			if _, err := q.Add(ctx, "after-pause", idleData{}); err != nil {
				t.Fatal(err)
			}
			watch := 300 * time.Millisecond
			if mode.sliced {
				watch += blockingReadSlice
			}
			for deadline := time.Now().Add(watch); time.Now().Before(deadline); {
				if active := listOf(t, c, "bull:"+name+":active"); len(active) != 0 {
					t.Fatalf("active = %v after PauseAndWait, want the job untouched in wait", active)
				}
				time.Sleep(20 * time.Millisecond)
			}
			mode.check(t, w)
		})
	}
}

// Pause can land at any point of the pinned read: before the connection is
// pinned, before the server has started blocking, or mid-block. It has to end
// the read every time; one missed unblock would hold the worker for the full
// five-second DrainDelay.
func TestPauseEndsPinnedReadAtAnyPoint(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		return "ok", nil
	}, nil)
	defer w.Close()

	for i := 0; i < 24; i++ {
		// Pause at a different point of the read each round.
		time.Sleep(time.Duration(i%8) * 500 * time.Microsecond)
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := w.PauseAndWait(waitCtx)
		cancel()
		if err != nil {
			t.Fatalf("round %d: PauseAndWait: %v", i, err)
		}
		w.Resume()
		waitUntil(t, 2*time.Second, "the resumed worker to block again", func() bool { return !w.blockingReadIdle() })
	}
	if e := errs.all(); len(e) != 0 {
		t.Fatalf("worker errors: %v", e)
	}
	pinnedMode.check(t, w)
}

// Close and Shutdown on an idle worker must leave a job that arrives during
// the close in the wait list for other workers, not stranded in active. On a
// pinned connection they also return at once, well inside a Shutdown budget
// shorter than a second.
func TestClosingIdleWorkerLeavesNewJobWaiting(t *testing.T) {
	type closer func(w *Worker[idleData, string], budget time.Duration) error
	for _, tc := range []struct {
		name  string
		close closer
	}{
		{"Close", func(w *Worker[idleData, string], _ time.Duration) error { return w.Close() }},
		{"Shutdown", func(w *Worker[idleData, string], budget time.Duration) error {
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			return w.Shutdown(ctx)
		}},
	} {
		for _, mode := range readModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				ctx := context.Background()
				c := testClient(t)
				name := testQueueName(t, c)
				prefix := "bull:" + name + ":"
				q, err := NewQueue[idleData](name, c, nil)
				if err != nil {
					t.Fatal(err)
				}

				var processed atomic.Int64
				w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
					processed.Add(1)
					return "ok", nil
				}, nil, mode.setup)

				// The sliced read needs up to a second to end; the pinned one
				// must not need anywhere near that.
				budget := 400 * time.Millisecond
				if mode.sliced {
					budget = blockingReadSlice + time.Second
				}
				closeWorker := tc.close
				closed := make(chan error, 1)
				start := time.Now()
				go func() { closed <- closeWorker(w, budget) }()
				waitUntil(t, time.Second, "the close to begin", w.closing.Load)
				job, err := q.Add(ctx, "during-close", idleData{})
				if err != nil {
					t.Fatal(err)
				}

				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("%s on an idle worker: %v", tc.name, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("%s did not return", tc.name)
				}
				if took := time.Since(start); took > budget {
					t.Fatalf("%s on an idle worker took %s, want under %s", tc.name, took, budget)
				}

				if n := processed.Load(); n != 0 {
					t.Fatalf("closing worker processed %d job(s)", n)
				}
				if wait := listOf(t, c, prefix+"wait"); len(wait) != 1 || wait[0] != job.ID() {
					t.Fatalf("wait = %v, want [%s]", wait, job.ID())
				}
				if active := listOf(t, c, prefix+"active"); len(active) != 0 {
					t.Fatalf("active = %v, want empty", active)
				}
				if e := errs.all(); len(e) != 0 {
					t.Fatalf("worker errors: %v", e)
				}
				mode.check(t, w)
			})
		}
	}
}

// A worker that can never use the pinned read must notice on its own, stop
// trying, and still work, waiting in slices.
func TestIdleWorkerFallsBackToSlicedReads(t *testing.T) {
	onePooled := func(t *testing.T) redis.Cmdable {
		c := testClient(t)
		opt := *c.Options()
		opt.PoolSize = 1
		single := redis.NewClient(&opt)
		t.Cleanup(func() { _ = single.Close() })
		return single
	}
	refusing := func(subcommand, reply string) func(t *testing.T) redis.Cmdable {
		return func(t *testing.T) redis.Cmdable {
			c := testClient(t)
			c.AddHook(&refuseCmd{subcommand: subcommand, err: replyError(reply)})
			return c
		}
	}
	for _, tc := range []struct {
		name   string
		client func(t *testing.T) redis.Cmdable
	}{
		{"pool of one connection", onePooled},
		{"a single connection", func(t *testing.T) redis.Cmdable {
			conn := testClient(t).Conn()
			t.Cleanup(func() { _ = conn.Close() })
			return conn
		}},
		{"no CLIENT ID", refusing("id", "ERR unknown subcommand 'id'. Try CLIENT HELP.")},
		{"no CLIENT UNBLOCK", refusing("unblock", "ERR unknown subcommand 'unblock'. Try CLIENT HELP.")},
		{"CLIENT UNBLOCK forbidden", refusing("unblock", "NOPERM this user has no permissions to run the 'client|unblock' command")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := tc.client(t)
			name := testQueueName(t, testClient(t))
			q, err := NewQueue[idleData](name, testClient(t), nil)
			if err != nil {
				t.Fatal(err)
			}

			ran := make(chan struct{}, 1)
			w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
				ran <- struct{}{}
				return "ok", nil
			}, nil)
			defer w.Close()
			slicedMode.check(t, w)

			if _, err := q.Add(ctx, "job", idleData{}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ran:
			case <-time.After(5 * time.Second):
				t.Fatalf("job did not run (worker errors: %v)", errs.all())
			}
			if e := errs.all(); len(e) != 0 {
				t.Fatalf("worker errors: %v", e)
			}
		})
	}
}

// A pinned read keeps its connection for the whole wait, so workers that
// share one pool may only take up half of it that way. The rest wait in
// slices and leave the pool in rotation for everything else on the client.
func TestBusyPoolKeepsWorkersOffPinnedReads(t *testing.T) {
	ctx := context.Background()
	base := testClient(t)
	opt := *base.Options()
	opt.PoolSize = 4
	shared := redis.NewClient(&opt)
	t.Cleanup(func() { _ = shared.Close() })
	counter := &cmdCounter{}
	shared.AddHook(counter)

	const workers = 4
	ran := make(chan string, workers)
	var names []string
	var all []*Worker[idleData, string]
	var logs []*errorLog
	defer func() {
		var closing sync.WaitGroup
		for _, w := range all {
			closing.Add(1)
			go func(w *Worker[idleData, string]) {
				defer closing.Done()
				_ = w.Close()
			}(w)
		}
		closing.Wait()
	}()
	for i := 0; i < workers; i++ {
		name := testQueueName(t, base)
		// A worker decides how to wait from how much of the pool is in use
		// at that moment. The stalled check it runs when it starts would add
		// a connection to that count, and an earlier worker's read timing
		// out would take one away, so the check is skipped and the wait is
		// set to its maximum.
		opts := &WorkerOptions{SkipStalledCheck: true, DrainDelay: maxBlockTimeout}
		w, errs := runIdleWorker(t, name, shared, func(ctx context.Context, job *Job[idleData]) (string, error) {
			ran <- job.Name()
			return "ok", nil
		}, opts)
		names, all, logs = append(names, name), append(all, w), append(logs, errs)
		// Once its read has gone out, the worker has chosen how to wait, and
		// a pinned read is holding the connection the next worker has to
		// count.
		waitUntil(t, 5*time.Second, "the worker to send its blocking read", func() bool {
			return counter.readSent(w.keyPrefix + "wait")
		})
	}

	// Half of a pool of four is two.
	for i, w := range all {
		want := pinnedReadUntried
		if i < 2 {
			want = pinnedReadOn
		}
		if got := w.pinnedRead.Load(); got != want {
			t.Errorf("worker %d: pinnedRead = %d, want %d", i+1, got, want)
		}
	}
	if n := counter.clientID.Load(); n != 2 {
		t.Errorf("%d connections pinned, want 2", n)
	}

	// The pool still serves everyone.
	for i, name := range names {
		q, err := NewQueue[idleData](name, shared, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.Add(ctx, name, idleData{}); err != nil {
			t.Fatalf("adding to queue %d: %v", i+1, err)
		}
	}
	for i := 0; i < workers; i++ {
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d jobs ran", i, workers)
		}
	}
	for i, errs := range logs {
		if e := errs.all(); len(e) != 0 {
			t.Errorf("worker %d errors: %v", i+1, e)
		}
	}
}

// Losing the connection while setting up a pinned read is not the worker's
// error to report: the wait goes through the sliced read, which finds out
// for itself whether the server is there.
func TestPinnedReadSetupFailureIsQuiet(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	c.AddHook(&refuseCmd{subcommand: "id", err: io.ErrUnexpectedEOF})
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}

	ran := make(chan struct{}, 1)
	w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		ran <- struct{}{}
		return "ok", nil
	}, nil)
	defer w.Close()

	if _, err := q.Add(ctx, "job", idleData{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatalf("job did not run (worker errors: %v)", errs.all())
	}
	if e := errs.all(); len(e) != 0 {
		t.Fatalf("worker errors: %v", e)
	}
	// Not a refusal, so the next wait may try again.
	if got := w.pinnedRead.Load(); got != pinnedReadUntried {
		t.Fatalf("pinnedRead = %d, want it still untried", got)
	}
}

// An error reply that will pass (the server is loading, say) sends one wait
// through the sliced read without giving up on the pinned one.
func TestPinnedReadSurvivesPassingServerErrors(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	hook := &refuseCmd{subcommand: "id", err: replyError("LOADING Redis is loading the dataset in memory"), limit: 1}
	c.AddHook(hook)
	counter := &cmdCounter{}
	c.AddHook(counter)
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}

	ran := make(chan struct{}, 2)
	w, err := NewWorker[idleData, string](name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		ran <- struct{}{}
		return "ok", nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	errs := &errorLog{}
	w.OnError(errs.add)
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// The first wait runs in slices; a job ends it, and the next one pins.
	waitUntil(t, 5*time.Second, "the refused wait to start", func() bool { return hook.refused.Load() > 0 })
	for i := 0; i < 2; i++ {
		if _, err := q.Add(ctx, "job", idleData{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatalf("job %d did not run (worker errors: %v)", i+1, errs.all())
		}
		waitUntil(t, 5*time.Second, "the worker to block again", func() bool { return w.drained.Load() && !w.blockingReadIdle() })
	}
	waitUntil(t, 5*time.Second, "the worker to pin a connection", func() bool { return w.pinnedRead.Load() == pinnedReadOn })
	if e := errs.all(); len(e) != 0 {
		t.Fatalf("worker errors: %v", e)
	}
}

// Only a reply that says the server will never run the command turns the
// pinned read off for good.
func TestIsUnsupported(t *testing.T) {
	for _, tc := range []struct {
		err                error
		reply, unsupported bool
	}{
		{replyError("ERR unknown command 'CLIENT'"), true, true},
		{replyError("NOPERM this user has no permissions to run the 'client|id' command"), true, true},
		{replyError("LOADING Redis is loading the dataset in memory"), true, false},
		{replyError("BUSY Redis is busy running a script."), true, false},
		{replyError("MOVED 3999 127.0.0.1:7002"), true, false},
		{redis.Nil, false, false},
		{context.Canceled, false, false},
		{net.ErrClosed, false, false},
		{nil, false, false},
	} {
		if got := isReplyError(tc.err); got != tc.reply {
			t.Errorf("isReplyError(%v) = %t, want %t", tc.err, got, tc.reply)
		}
		if got := isUnsupported(tc.err); got != tc.unsupported {
			t.Errorf("isUnsupported(%v) = %t, want %t", tc.err, got, tc.unsupported)
		}
	}
}

// Closing a worker that is waiting out a rate limit is not an error.
func TestCloseDuringRateLimitDelayIsQuiet(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	q, err := NewQueue[idleData](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}

	w, errs := runIdleWorker(t, name, c, func(ctx context.Context, job *Job[idleData]) (string, error) {
		return "ok", nil
	}, &WorkerOptions{Limiter: &RateLimiterOptions{Max: 1, Duration: 60000}})
	if err := w.RateLimit(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Add(ctx, "held", idleData{}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the worker to hit the rate limit", func() bool {
		return w.limitUntil.Load() > time.Now().UnixMilli()
	})

	start := time.Now()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > minQueueCleanupTimeout+time.Second {
		t.Fatalf("Close took %s while rate limited", took)
	}
	if e := errs.all(); len(e) != 0 {
		t.Fatalf("worker errors: %v", e)
	}
}

// Every empty reply refreshes blockUntil, so a delayed job that was removed
// or run by another worker cannot leave a stale deadline behind.
func TestEmptyReplyClearsStaleBlockUntil(t *testing.T) {
	c := testClient(t)
	w, err := NewWorker[idleData, string](testQueueName(t, c), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.drained.Store(true)
	w.blockUntil.Store(time.Now().Add(-time.Minute).UnixMilli())

	if _, err := w.nextJobFromJobData(context.Background(), nil, "0", 0, 0, "token"); err != nil {
		t.Fatal(err)
	}
	if until := w.blockUntil.Load(); until != 0 {
		t.Fatalf("blockUntil = %d after an empty reply with no delayed job, want 0", until)
	}

	next := time.Now().Add(time.Minute).UnixMilli()
	if _, err := w.nextJobFromJobData(context.Background(), nil, "0", 0, next, "token"); err != nil {
		t.Fatal(err)
	}
	if until := w.blockUntil.Load(); until != next {
		t.Fatalf("blockUntil = %d, want the reply's next delayed timestamp %d", until, next)
	}
}

// ReturnToWait undoes the blocking read: the ID goes back to the end of the
// list it was popped from, or to the paused list if the queue paused since.
func TestReturnToWait(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	prefix := "bull:" + testQueueName(t, c) + ":"

	// addJob LPUSHes, so the right end is the oldest job and the next to pop.
	if err := c.LPush(ctx, prefix+"wait", "a", "b").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.LPush(ctx, prefix+"active", "x", "other").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.SAdd(ctx, prefix+"stalled", "x").Err(); err != nil {
		t.Fatal(err)
	}

	if err := lua.ReturnToWait(ctx, c, prefix, "x"); err != nil {
		t.Fatal(err)
	}
	if wait := listOf(t, c, prefix+"wait"); strings.Join(wait, ",") != "b,a,x" {
		t.Fatalf("wait = %v, want x back at the popping end: [b a x]", wait)
	}
	if active := listOf(t, c, prefix+"active"); strings.Join(active, ",") != "other" {
		t.Fatalf("active = %v, want [other]", active)
	}
	if stalled, _ := c.SIsMember(ctx, prefix+"stalled", "x").Result(); stalled {
		t.Fatal("x is still marked as a stall candidate")
	}

	// Not in active any more: nothing to undo.
	if err := lua.ReturnToWait(ctx, c, prefix, "x"); err != nil {
		t.Fatal(err)
	}
	if wait := listOf(t, c, prefix+"wait"); strings.Join(wait, ",") != "b,a,x" {
		t.Fatalf("wait = %v after a no-op return, want it unchanged", wait)
	}

	// A paused queue keeps its jobs in the paused list.
	if err := c.HSet(ctx, prefix+"meta", "paused", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lua.ReturnToWait(ctx, c, prefix, "other"); err != nil {
		t.Fatal(err)
	}
	if paused := listOf(t, c, prefix+"paused"); strings.Join(paused, ",") != "other" {
		t.Fatalf("paused = %v, want [other]", paused)
	}
}
