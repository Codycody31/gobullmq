package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/fifoqueue"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/utils"
	backoffutil "go.codycody31.dev/gobullmq/internal/utils/backoff"
)

// ProcessFunc is the callback invoked for each job.
// D is the type of the job's data payload, R is the result type.
type ProcessFunc[D any, R any] func(ctx context.Context, job *Job[D]) (R, error)

// Worker processes jobs from a queue.
// D is the type of job data, R is the type of job results.
type Worker[D any, R any] struct {
	name         string
	token        uuid.UUID
	ee           *eventemitter.EventEmitter
	running      atomic.Bool
	closing      atomic.Bool
	forceClosing atomic.Bool
	paused       atomic.Bool
	redisClient  redis.Cmdable
	// runCtx is the run-lifetime context created by Run and cancelled by
	// Close/Shutdown. Storing a context in a struct is normally discouraged,
	// but a worker is a long-lived process whose background goroutines,
	// timers, and Redis calls all share one lifetime, the documented
	// exception to that rule.
	runCtx    context.Context
	cancel    context.CancelFunc
	prefix    string
	keyPrefix string
	mutex     sync.Mutex
	wg        sync.WaitGroup
	opts      WorkerOptions
	processFn ProcessFunc[D, R]

	extendLocksTimer  *time.Timer
	stalledCheckTimer *time.Timer

	jobsInProgress *jobsInProgress

	blockUntil atomic.Int64
	// limitUntil holds an absolute epoch-ms deadline for the rate-limit
	// window (0 when unlimited).
	limitUntil atomic.Int64
	drained    atomic.Bool

	// blockingFetch serializes the blocking wait-list read so at most one
	// BLMOVE blocks at a time (upstream's this.waiting guard).
	blockingFetch sync.Mutex
	// waitCancel aborts an in-flight blocking read (upstream force-disconnects
	// the blocking connection on pause/close).
	waitCancelMu sync.Mutex
	waitCancel   context.CancelFunc

	scripts *scripts

	asyncFifoQueue *fifoqueue.FifoQueue[rawJob]

	pauseCh chan struct{}
}

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	Autorun          bool
	Concurrency      int
	Limiter          *RateLimiterOptions
	Metrics          *MetricsOptions
	Prefix           string
	MaxStalledCount  int
	StalledInterval  time.Duration
	RemoveOnComplete *KeepJobs
	RemoveOnFail     *KeepJobs
	SkipStalledCheck bool
	SkipLockRenewal  bool
	DrainDelay       time.Duration
	LockDuration     time.Duration
	LockRenewTime    time.Duration
	RunRetryDelay    time.Duration
	Backoff          *BackoffOptions
	BackoffStrategy  BackoffStrategyFunc
}

// BackoffStrategyFunc is a custom function to calculate backoff delay in milliseconds.
// Return 0 for immediate retry, or a positive value for the delay.
// Return -1 to signal "do not retry" (move to failed).
type BackoffStrategyFunc func(attemptsMade int, opts *BackoffOptions, err error, jobID string) int

// RateLimiterOptions caps the worker at Max jobs per Duration milliseconds.
type RateLimiterOptions struct {
	Max      int `msgpack:"max"`
	Duration int `msgpack:"duration"`
}

// MetricsOptions enables collection of completed/failed job counts, keeping
// up to MaxDataPoints one-minute data points.
type MetricsOptions struct {
	MaxDataPoints int
}

type nextJobOptions struct {
	Block bool
}

type jobsInProgress struct {
	sync.Mutex
	jobs map[string]jobInProgress
}

type jobInProgress struct {
	job rawJob
	ts  time.Time
}

// Name returns the queue name of the worker.
func (w *Worker[D, R]) Name() string {
	return w.name
}

// OnCompleted registers a typed callback for when a job completes and returns a ListenerID.
func (w *Worker[D, R]) OnCompleted(fn func(job *Job[D], result R)) ListenerID {
	return w.ee.On("completed", func(args ...any) {
		if len(args) >= 2 {
			if raw, ok := args[0].(rawJob); ok {
				typed, _ := wrapRawJob[D](&raw)
				if typed != nil {
					if r, ok := args[1].(R); ok {
						fn(typed, r)
					}
				}
			}
		}
	})
}

// OnFailed registers a typed callback for when a job fails and returns a ListenerID.
func (w *Worker[D, R]) OnFailed(fn func(job *Job[D], err error)) ListenerID {
	return w.ee.On("failed", func(args ...any) {
		if len(args) >= 2 {
			if raw, ok := args[0].(rawJob); ok {
				typed, _ := wrapRawJob[D](&raw)
				if typed != nil {
					if e, ok := args[1].(error); ok {
						fn(typed, e)
					}
				}
			}
		}
	})
}

// OnActive registers a typed callback for when a job becomes active and returns a ListenerID.
func (w *Worker[D, R]) OnActive(fn func(job *Job[D])) ListenerID {
	return w.ee.On("active", func(args ...any) {
		if len(args) >= 1 {
			if raw, ok := args[0].(rawJob); ok {
				typed, _ := wrapRawJob[D](&raw)
				if typed != nil {
					fn(typed)
				}
			}
		}
	})
}

// OnStalled registers a typed callback for when a job is detected as stalled and returns a ListenerID.
func (w *Worker[D, R]) OnStalled(fn func(jobID string)) ListenerID {
	return w.ee.On("stalled", func(args ...any) {
		if len(args) >= 1 {
			if id, ok := args[0].(string); ok {
				fn(id)
			}
		}
	})
}

// OnDrained registers a typed callback for when the queue is drained and returns a ListenerID.
func (w *Worker[D, R]) OnDrained(fn func()) ListenerID {
	return w.ee.On("drained", func(...any) {
		fn()
	})
}

// OnError registers a typed callback for worker errors and returns a ListenerID.
func (w *Worker[D, R]) OnError(fn func(err error)) ListenerID {
	return w.ee.On("error", func(args ...any) {
		if len(args) >= 1 {
			switch e := args[0].(type) {
			case error:
				fn(e)
			case string:
				fn(errors.New(e))
			default:
				fn(fmt.Errorf("%v", e))
			}
		}
	})
}

// OnRetriesExhausted registers a typed callback for when all retry attempts are exhausted and returns a ListenerID.
func (w *Worker[D, R]) OnRetriesExhausted(fn func(job *Job[D], err error)) ListenerID {
	return w.ee.On("retries-exhausted", func(args ...any) {
		if len(args) >= 2 {
			if raw, ok := args[0].(rawJob); ok {
				typed, _ := wrapRawJob[D](&raw)
				if typed != nil {
					if e, ok := args[1].(error); ok {
						fn(typed, e)
					}
				}
			}
		}
	})
}

// nextJobData represents the structured data returned by raw2NextJobData.
type nextJobData struct {
	JobData    map[string]any
	ID         string
	LimitUntil int64
	DelayUntil int64
}

// Default timing configuration
const (
	defaultLockDuration    = 30 * time.Second
	defaultLockRenewTime   = 15 * time.Second
	defaultStalledInterval = 30 * time.Second
	defaultRunRetryDelay   = 15 * time.Second // upstream runRetryDelay: 15000
	defaultDrainDelay      = 5 * time.Second  // upstream drainDelay: 5
	minQueueCleanupTimeout = time.Second
)

// NewWorker creates a new Worker instance.
// opts may be nil, in which case sensible defaults are used.
func NewWorker[D any, R any](name string, client redis.Cmdable, processor ProcessFunc[D, R], opts *WorkerOptions) (*Worker[D, R], error) {
	if name == "" {
		return nil, fmt.Errorf("worker name must be provided")
	}
	if client == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	if opts == nil {
		opts = &WorkerOptions{}
	}

	if opts.LockDuration <= 0 {
		opts.LockDuration = defaultLockDuration
	}
	if opts.LockRenewTime <= 0 || opts.LockRenewTime >= opts.LockDuration {
		opts.LockRenewTime = opts.LockDuration / 2
	}
	if opts.StalledInterval <= 0 {
		opts.StalledInterval = defaultStalledInterval
	}
	if opts.MaxStalledCount == 0 {
		opts.MaxStalledCount = 1
	} else if opts.MaxStalledCount < 0 {
		opts.MaxStalledCount = 0
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.RunRetryDelay <= 0 {
		opts.RunRetryDelay = defaultRunRetryDelay
	}
	if opts.DrainDelay <= 0 {
		opts.DrainDelay = defaultDrainDelay
	}

	w := &Worker[D, R]{
		name:        name,
		token:       uuid.New(),
		ee:          eventemitter.NewEventEmitter(),
		redisClient: client,
		opts:        *opts,
		processFn:   processor,
		jobsInProgress: &jobsInProgress{
			jobs: make(map[string]jobInProgress),
		},
		pauseCh: make(chan struct{}, 1),
	}

	if opts.Prefix == "" {
		w.keyPrefix = "bull"
	} else {
		w.keyPrefix = strings.Trim(opts.Prefix, ":")
		if w.keyPrefix == "" {
			return nil, fmt.Errorf("prefix cannot be empty or just colons")
		}
	}
	w.prefix = w.keyPrefix
	w.keyPrefix = w.keyPrefix + ":" + name + ":"

	w.scripts = newScripts(w.redisClient, w.keyPrefix)
	w.scripts.limiter = opts.Limiter
	w.scripts.removeOnComplete = opts.RemoveOnComplete
	w.scripts.removeOnFail = opts.RemoveOnFail

	return w, nil
}

// emit emits the event with the given name and arguments.
func (w *Worker[D, R]) emit(event string, args ...any) {
	w.ee.Emit(event, args...)
}

// Off removes a specific listener by its ListenerID.
func (w *Worker[D, R]) Off(event string, id ListenerID) {
	w.ee.RemoveListener(event, id)
}

// On listens for the event and returns a ListenerID that can be used with Off.
func (w *Worker[D, R]) On(event string, listener func(...any)) ListenerID {
	return w.ee.On(event, listener)
}

// Once listens for the event only once and returns a ListenerID.
func (w *Worker[D, R]) Once(event string, listener func(...any)) ListenerID {
	return w.ee.Once(event, listener)
}

// createJob constructs a rawJob from the map data retrieved from Redis.
func (w *Worker[D, R]) createJob(jobData map[string]any, jobId string) (rawJob, error) {
	job, err := jobFromJson(jobData)
	if err != nil {
		return rawJob{}, fmt.Errorf("failed to deserialize job %s: %w", jobId, err)
	}
	job.id = jobId
	job.setJobContext(w.redisClient, w.keyPrefix)
	return job, nil
}

// Run starts the worker with the given context.
func (w *Worker[D, R]) Run(ctx context.Context) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.running.Load() {
		return ErrWorkerRunning
	}
	if w.closing.Load() {
		return ErrWorkerClosing
	}

	w.running.Store(true)

	ctx, cancel := context.WithCancel(ctx)
	w.runCtx = ctx
	w.cancel = cancel

	clientName := fmt.Sprintf("%s:%s", w.prefix, base64.StdEncoding.EncodeToString([]byte(w.name)))
	switch c := w.redisClient.(type) {
	case *redis.Client:
		_ = c.Do(ctx, "CLIENT", "SETNAME", clientName).Err()
	case *redis.ClusterClient:
		_ = c.ForEachShard(ctx, func(ctx context.Context, shardClient *redis.Client) error {
			return shardClient.Do(ctx, "CLIENT", "SETNAME", clientName).Err()
		})
	}

	go w.startStalledCheckTimer()
	go w.startLockExtender()

	w.asyncFifoQueue = fifoqueue.NewFifoQueue[rawJob](w.opts.Concurrency, false)
	tokenPostfix := 0

	addFetchTask := func() {
		// Fetch tasks ARE re-added while paused: they park in getNextJob's
		// pause-wait loop and Resume wakes them. Refusing to re-add here
		// would leave a resumed worker with zero fetchers (livelock).
		if w.closing.Load() {
			return
		}
		tokenPostfix++
		token := fmt.Sprintf("%s:%d", w.token, tokenPostfix)
		if err := w.asyncFifoQueue.Add(func() (rawJob, error) {
			j, err := w.retryIfFailed(func() (*rawJob, error) {
				nextJob, err := w.getNextJob(w.runCtx, token, nextJobOptions{Block: true})
				if err != nil {
					return nil, err
				}
				if nextJob == nil {
					return nil, nil
				}
				nextJob.token = token
				return nextJob, nil
			}, w.opts.RunRetryDelay)

			if err != nil {
				w.emit("error", fmt.Errorf("error fetching job: %v", err))
				return rawJob{}, err
			}

			if j.id == "" {
				return rawJob{}, nil
			}

			return j, nil
		}); err != nil {
			w.emit("error", fmt.Errorf("error adding fetch task to queue: %v", err))
		}
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for i := 0; i < w.opts.Concurrency; i++ {
			addFetchTask()
		}

		for {
			if w.closing.Load() && w.asyncFifoQueue.NumTotal() == 0 {
				w.emit("closing")
				return
			}
			select {
			case <-w.runCtx.Done():
				w.emit("closing")
				return
			default:
			}

			jobResult, taskErr := w.asyncFifoQueue.Fetch(w.runCtx)

			if taskErr != nil {
				if errors.Is(taskErr, context.Canceled) || errors.Is(taskErr, fifoqueue.ErrQueueClosed) {
					w.emit("info", fmt.Sprintf("Worker stopping due to context cancellation or queue closure: %v", taskErr))
					return
				}
				w.emit("error", fmt.Errorf("error fetching task result from queue: %v", taskErr))
				addFetchTask()
				continue
			}

			if jobResult != nil && jobResult.id != "" && jobResult.id != "0" {
				fetchedJob := *jobResult
				token := fetchedJob.token
				w.trackJob(fetchedJob)

				if err := w.asyncFifoQueue.Add(func() (rawJob, error) {
					// Chain jobs handed back by moveToFinished's fetch-next
					// under the same token, like upstream's process loop;
					// otherwise the prefetched job is stranded in active.
					job := fetchedJob
					for job.id != "" && job.id != "0" {
						next, _ := w.processJob(
							job,
							token,
							func() bool {
								// The running process task counts in NumTotal
								// (upstream: numTotal() <= concurrency).
								return w.asyncFifoQueue.NumTotal() <= w.opts.Concurrency
							},
						)
						job = next
					}
					return rawJob{}, nil
				}); err != nil {
					w.untrackJob(fetchedJob.id)
					w.emit("error", fmt.Errorf("error adding process task for job %s: %v", fetchedJob.id, err))
					addFetchTask()
				}
			} else {
				addFetchTask()
			}
		}
	}()

	return nil
}

// getNextJob gets the next job.
func (w *Worker[D, R]) getNextJob(ctx context.Context, token string, opts nextJobOptions) (*rawJob, error) {
	if w.paused.Load() {
		if opts.Block {
			for w.paused.Load() && !w.closing.Load() {
				select {
				case <-w.pauseCh:
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return nil, nil
				}
			}
		} else {
			return nil, nil
		}
	}

	if w.closing.Load() {
		return nil, nil
	}

	if limitUntil := w.limitUntil.Load(); limitUntil > time.Now().UnixMilli() {
		if err := w.delay(ctx, limitUntil); err != nil {
			return nil, fmt.Errorf("failed to delay: %w", err)
		}
	}

	if w.drained.Load() && opts.Block {
		// One blocking read at a time (upstream this.waiting).
		w.blockingFetch.Lock()
		var jobID string
		// Re-check under the guard: Pause/Close may have landed while this
		// task waited for the previous blocking read to finish; starting a
		// fresh BLMOVE then would keep pulling jobs during a pause or a
		// Shutdown drain window.
		if !w.paused.Load() && !w.closing.Load() {
			var err error
			jobID, err = w.waitForJob(ctx)
			if err != nil {
				w.blockingFetch.Unlock()
				if !w.paused.Load() && !w.closing.Load() {
					return nil, fmt.Errorf("failed to wait for job: %w", err)
				}
				return nil, nil
			}
		}
		w.blockingFetch.Unlock()
		if jobID == "" && (w.paused.Load() || w.closing.Load()) {
			// Nothing was moved and we are pausing/closing: do not promote
			// or lock new work. (A non-empty jobID was already moved by the
			// BLMOVE server-side; locking it lets stalled recovery handle
			// the boundary, same as upstream's disconnect race.)
			return nil, nil
		}
		// A block timeout still proceeds to moveToActive: its Lua promotes
		// due delayed jobs, the only mechanism by which an idle worker
		// picks up delayed work.
		return w.moveToActive(ctx, token, jobID)
	}

	return w.moveToActive(ctx, token, "")
}

// moveToActive moves the job to the active list.
func (w *Worker[D, R]) moveToActive(ctx context.Context, token string, jobId string) (*rawJob, error) {
	if jobId != "" && len(jobId) > 2 && jobId[0:2] == "0:" {
		blockUntil, err := strconv.Atoi(jobId[2:])
		if err != nil {
			return nil, fmt.Errorf("failed to parse blockUntil: %w", err)
		}
		w.blockUntil.Store(int64(blockUntil))
	}

	keys := []string{
		w.keyPrefix + "wait",
		w.keyPrefix + "active",
		w.keyPrefix + "prioritized",
		w.keyPrefix + "events",
		w.keyPrefix + "stalled",
		w.keyPrefix + "limiter",
		w.keyPrefix + "delayed",
		w.keyPrefix + "paused",
		w.keyPrefix + "meta",
		w.keyPrefix + "pc",
	}

	opts := map[string]any{
		"token":        token,
		"lockDuration": int(w.opts.LockDuration / time.Millisecond),
	}
	if w.opts.Limiter != nil {
		opts["limiter"] = map[string]any{
			"max":      w.opts.Limiter.Max,
			"duration": w.opts.Limiter.Duration,
		}
	}

	msgPackedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal opts: %w", err)
	}

	rawResult, err := lua.MoveToActive(ctx, w.redisClient, keys, w.keyPrefix, time.Now().UnixMilli(), jobId, string(msgPackedOpts))
	if err != nil {
		return nil, err
	}

	rawResultSlice, ok := rawResult.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected type for rawResult: %T", rawResult)
	}

	result := raw2NextJobData(rawResultSlice)
	if result == nil {
		w.emit("error", fmt.Errorf("moveToActive received invalid data from Lua for token %s, jobID %s", token, jobId))
		return nil, nil
	}

	if result.ID == "0" || result.ID == "" {
		return nil, nil
	}

	return w.nextJobFromJobData(ctx, result.JobData, result.ID, result.LimitUntil, result.DelayUntil, token)
}

// raw2NextJobData processes raw data and returns a typed nextJobData structure.
func raw2NextJobData(raw []any) *nextJobData {
	if len(raw) < 4 {
		return nil
	}

	limitVal, limitOk := raw[2].(int64)
	delayVal, delayOk := raw[3].(int64)

	if !limitOk || !delayOk {
		return nil
	}

	result := &nextJobData{
		ID:         fmt.Sprintf("%v", raw[1]),
		LimitUntil: clampMin(limitVal, 0),
		DelayUntil: clampMin(delayVal, 0),
	}

	if raw[0] != nil {
		jobMap := utils.Array2obj(raw[0])
		result.JobData = jobMap
	}

	return result
}

// waitForJob waits for a job ID from the wait list using the
// BRPOPLPUSH-equivalent orientation (pop the oldest job from the right of
// wait, push to the left of active). A block timeout returns "" without
// error; Pause/Close abort the wait.
func (w *Worker[D, R]) waitForJob(ctx context.Context) (string, error) {
	blockTimeout := w.opts.DrainDelay
	if blockUntil := w.blockUntil.Load(); blockUntil > 0 {
		blockTimeout = time.Until(time.UnixMilli(blockUntil))
	}
	// go-redis only supports whole-second BLMOVE timeouts (and logs when
	// truncating), so clamp to [1s, 10s].
	if blockTimeout < time.Second {
		blockTimeout = time.Second
	}
	if blockTimeout > 10*time.Second {
		blockTimeout = 10 * time.Second
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.waitCancelMu.Lock()
	w.waitCancel = cancel
	w.waitCancelMu.Unlock()
	defer func() {
		w.waitCancelMu.Lock()
		w.waitCancel = nil
		w.waitCancelMu.Unlock()
	}()

	result, err := w.redisClient.BLMove(waitCtx, w.keyPrefix+"wait", w.keyPrefix+"active", "RIGHT", "LEFT", blockTimeout).Result()
	if err != nil {
		// Block timeout (redis.Nil) and an aborted wait are not errors.
		if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
			return "", nil
		}
		return "", err
	}
	return result, nil
}

// abortWait cancels an in-flight blocking read, if any.
func (w *Worker[D, R]) abortWait() {
	w.waitCancelMu.Lock()
	if w.waitCancel != nil {
		w.waitCancel()
	}
	w.waitCancelMu.Unlock()
}

// sleepContext sleeps for the specified duration or until context is cancelled.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// delay delays the execution for the specified time, respecting context cancellation.
func (w *Worker[D, R]) delay(ctx context.Context, until int64) error {
	now := time.Now().UnixMilli()
	if until > now {
		return sleepContext(ctx, time.Duration(until-now)*time.Millisecond)
	}
	return nil
}

// nextJobFromJobData processes the next job data and returns a rawJob.
func (w *Worker[D, R]) nextJobFromJobData(
	ctx context.Context,
	jobData map[string]any,
	jobID string,
	limitUntil int64,
	delayUntil int64,
	token string,
) (*rawJob, error) {
	if jobData == nil {
		if !w.drained.Load() {
			w.emit("drained")
			w.drained.Store(true)
			w.blockUntil.Store(0)
		}
	}

	// The Lua returns limitUntil as a relative PTTL; store an absolute
	// deadline so delay() sleeps for the actual window.
	if limitUntil > 0 {
		w.limitUntil.Store(time.Now().UnixMilli() + limitUntil)
	} else {
		w.limitUntil.Store(0)
	}
	if delayUntil > 0 {
		w.blockUntil.Store(clampMin(delayUntil, 0))
	}

	if jobData == nil {
		return nil, nil
	}

	w.drained.Store(false)
	job, err := w.createJob(jobData, jobID)
	if err != nil {
		return nil, err
	}
	job.token = token
	if job.opts.Repeat != nil {
		// Schedule the next iteration at fetch time, before processing, so a
		// failing iteration does not kill the chain. The existence check
		// (skipCheckExists=false) lets RemoveRepeatable stop the chain.
		// A scheduling failure aborts the fetch like upstream (the job stays
		// in active for stalled recovery) instead of silently ending the chain.
		jsonData, _ := job.data.(string)
		if _, err := addNextRepeatableJobInternal(ctx, w.redisClient, w.keyPrefix, job.name, jsonData, job.opts, false); err != nil {
			return nil, fmt.Errorf("failed to schedule next repeatable iteration for job %s: %w", job.id, err)
		}
	}
	return &job, nil
}

// processJob processes the job.
func (w *Worker[D, R]) processJob(job rawJob, token string, fetchNextCallback func() bool) (rawJob, error) {
	if w.forceClosing.Load() {
		w.untrackJob(job.id)
		return rawJob{}, nil
	}

	w.trackJob(job)
	defer w.untrackJob(job.id)

	w.emit("active", job, "waiting")

	var result R
	var err error

	processFnCtx, processFnCtxCancel := context.WithCancel(w.runCtx)
	defer processFnCtxCancel()

	func() {
		defer func() {
			if r := recover(); r != nil {
				w.emit("error", fmt.Errorf("panic recovered for job %s with token %s: %v", job.id, token, r))
				err = fmt.Errorf("panic: %v", r)
			}
		}()

		typedJob, wrapErr := wrapRawJob[D](&job)
		if wrapErr != nil {
			err = fmt.Errorf("failed to deserialize job data: %w", wrapErr)
			return
		}
		result, err = w.processFn(processFnCtx, typedJob)
	}()

	if err != nil {
		return w.handleJobError(job, token, err)
	}

	return w.handleJobSuccess(job, token, result, fetchNextCallback)
}

func (w *Worker[D, R]) trackJob(job rawJob) {
	if job.id == "" || job.id == "0" {
		return
	}
	w.jobsInProgress.Lock()
	if _, exists := w.jobsInProgress.jobs[job.id]; !exists {
		w.jobsInProgress.jobs[job.id] = jobInProgress{job: job, ts: time.Now()}
	}
	w.jobsInProgress.Unlock()
}

func (w *Worker[D, R]) untrackJob(jobID string) {
	if jobID == "" || jobID == "0" {
		return
	}
	w.jobsInProgress.Lock()
	delete(w.jobsInProgress.jobs, jobID)
	w.jobsInProgress.Unlock()
}

// handleJobError handles the error path of processJob. The fail path never
// prefetches (upstream job.moveToFailed defaults fetchNext to false).
func (w *Worker[D, R]) handleJobError(job rawJob, token string, err error) (rawJob, error) {
	if errors.Is(err, ErrRateLimit) {
		if w.scripts != nil {
			pttl, mErr := w.scripts.moveJobFromActiveToWait(w.runCtx, job.id, token)
			if mErr != nil {
				w.emit("error", fmt.Errorf("moveJobFromActiveToWait failed for %s: %v", job.id, mErr))
			} else {
				w.limitUntil.Store(time.Now().Add(time.Duration(pttl) * time.Millisecond).UnixMilli())
			}
		}
		return rawJob{}, err
	}

	if errors.Is(err, ErrDelayed) || errors.Is(err, ErrWaitingChildren) {
		return rawJob{}, err
	}

	// failedReason and the stacktrace are persisted atomically with the state
	// move below, like upstream's single multi in job.moveToFailed.
	stacktrace, stErr := job.prepareStacktrace(err)
	if stErr != nil {
		w.emit("error", fmt.Errorf("failed to marshal stacktrace for job %s: %v", job.id, stErr))
		return rawJob{}, err
	}

	// An automatic retry is performed unless attempts are exhausted, the job
	// was discarded, or the error is marked unrecoverable (upstream job.ts:622).
	if job.attemptsMade < job.opts.Attempts && !job.discarded && !errors.Is(err, ErrUnrecoverable) {
		// The job's own backoff wins; WorkerOptions.Backoff is the default for
		// jobs without one (like queue defaultJobOptions.backoff upstream).
		backoff := job.opts.Backoff
		if backoff == nil {
			backoff = w.opts.Backoff
		}
		delayMs := 0
		if backoff != nil {
			if w.opts.BackoffStrategy != nil && !backoffutil.IsBuiltin(backoff.Type) {
				// Custom strategies handle non-builtin types, like upstream
				// settings.backoffStrategy in lookupStrategy.
				delayMs = w.opts.BackoffStrategy(job.attemptsMade, backoff, err, job.id)
			} else {
				var backoffErr error
				delayMs, backoffErr = backoffutil.Calculate(backoffutil.Options{Type: backoff.Type, Delay: backoff.Delay}, job.attemptsMade)
				if backoffErr != nil {
					// Upstream throws before any Redis write: the job stays in
					// active and the stalled checker recovers it.
					w.emit("error", fmt.Errorf("backoff calculation failed for job %s: %v", job.id, backoffErr))
					return rawJob{}, err
				}
			}
		}
		if delayMs >= 0 {
			var opErr error
			jobKey := w.keyPrefix + job.id
			if delayMs > 0 {
				keys, args := w.scripts.moveToDelayedArgs(job.id, time.Now().UnixMilli()+int64(delayMs), token)
				result, derr := runFailureMulti(w.runCtx, w.redisClient, jobKey, stacktrace, job.failedReason, lua.MoveToDelayedScript, keys, args)
				opErr = derr
				if opErr == nil {
					if code, ok := result.(int64); ok && code < 0 {
						opErr = finishedErrors(code, job.id, "moveToDelayed")
					}
				}
			} else {
				keys, args := w.scripts.retryJobArgs(job.id, job.opts.Lifo, token)
				result, rerr := runFailureMulti(w.runCtx, w.redisClient, jobKey, stacktrace, job.failedReason, lua.RetryJobScript, keys, args)
				opErr = rerr
				if opErr == nil {
					if code, ok := result.(int64); ok && code < 0 {
						opErr = finishedErrors(code, job.id, "retryJob")
					}
				}
			}
			if opErr != nil {
				// Like upstream handleFailed's catch: emit only; a worker will
				// (or already has) recover the job through the stalled path.
				w.emit("error", fmt.Errorf("failed to schedule retry for job %s: %v", job.id, opErr))
				return rawJob{}, err
			}
			w.emit("failed", job, err, "active")
			return rawJob{}, err
		}
		// delayMs < 0: the strategy vetoed the retry; fall through to failed.
	}

	if job.opts.Attempts > 0 && job.attemptsMade >= job.opts.Attempts {
		w.emit("retries-exhausted", job, err)
	}
	lockDurationMs := int(w.opts.LockDuration / time.Millisecond)
	maxMetricsSize := ""
	if w.opts.Metrics != nil && w.opts.Metrics.MaxDataPoints > 0 {
		maxMetricsSize = strconv.Itoa(w.opts.Metrics.MaxDataPoints)
	}
	if moveErr := jobMoveToFailed(w.runCtx, w.scripts, &job, stacktrace, token, false, lockDurationMs, maxMetricsSize); moveErr != nil {
		w.emit("error", fmt.Errorf("error explicitly moving job %s to failed: %v", job.id, moveErr))
	}
	w.emit("failed", job, err, "active")
	return rawJob{}, err
}

// handleJobSuccess handles the success path of processJob.
func (w *Worker[D, R]) handleJobSuccess(job rawJob, token string, result R, fetchNextCallback func() bool) (rawJob, error) {
	lockDurationMs := int(w.opts.LockDuration / time.Millisecond)
	maxMetricsSize := ""
	if w.opts.Metrics != nil && w.opts.Metrics.MaxDataPoints > 0 {
		maxMetricsSize = strconv.Itoa(w.opts.Metrics.MaxDataPoints)
	}

	job.returnValue = result
	stringifiedReturnValue, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		w.emit("error", fmt.Errorf("error marshaling result for job %s: %v", job.id, marshalErr))
		return rawJob{}, marshalErr
	}

	getNext := fetchNextCallback() && !w.closing.Load() && !w.paused.Load()
	keys, args, scriptErr := w.scripts.moveToFinishedArgs(&job, string(stringifiedReturnValue), "returnvalue", job.opts.RemoveOnComplete, "completed", token, time.Now(), getNext, lockDurationMs, maxMetricsSize)
	if scriptErr != nil {
		w.emit("error", fmt.Errorf("error moving job to completed: %v", scriptErr))
		return rawJob{}, scriptErr
	}

	job.finishedOn = time.Now()
	rawLuaResult, luaErr := lua.MoveToFinished(w.runCtx, w.redisClient, keys, args...)
	if luaErr != nil {
		w.emit("error", fmt.Errorf("error moving job to completed: %v", luaErr))
		return rawJob{}, luaErr
	}

	var completed []any
	switch v := rawLuaResult.(type) {
	case int64:
		completed = []any{v}
	case []any:
		completed = v
	default:
		return rawJob{}, fmt.Errorf("unexpected type for rawResult: %T", rawLuaResult)
	}

	if code, ok := completed[0].(int64); ok && code < 0 {
		return rawJob{}, finishedErrors(code, job.id, "moveToFinished")
	}

	w.emit("completed", job, result, "active")

	nextData := raw2NextJobData(completed)
	if nextData != nil {
		j, nextErr := w.nextJobFromJobData(w.runCtx, nextData.JobData, nextData.ID, nextData.LimitUntil, nextData.DelayUntil, token)
		if nextErr != nil {
			w.emit("error", fmt.Errorf("error getting next job: %v", nextErr))
			return rawJob{}, nextErr
		}
		if j != nil {
			return *j, nil
		}
	}
	return rawJob{}, nil
}

// NextJob manually fetches the next waiting job and moves it to active,
// for workers used without Run() (upstream's manual processing pattern).
// token identifies the lock owner and must be passed to the job's
// MoveToCompleted/MoveToFailed calls (the returned job carries it).
// Returns nil when no job is available.
func (w *Worker[D, R]) NextJob(ctx context.Context, token string) (*Job[D], error) {
	raw, err := w.getNextJob(ctx, token, nextJobOptions{Block: false})
	if err != nil || raw == nil {
		return nil, err
	}
	raw.token = token
	return wrapRawJob[D](raw)
}

// RateLimit overrides the rate limit to be active for the given duration,
// mirroring upstream worker.rateLimit(expireTimeMs). A processor typically
// calls this and then returns ErrRateLimit.
func (w *Worker[D, R]) RateLimit(ctx context.Context, d time.Duration) error {
	const maxSafeInteger = int64(9007199254740991) // Number.MAX_SAFE_INTEGER
	return w.redisClient.Set(ctx, w.keyPrefix+"limiter", maxSafeInteger, d).Err()
}

// Pause pauses processing of this worker immediately: no new jobs are
// fetched, but jobs already in progress keep running. Use PauseAndWait to
// also wait for in-flight jobs to finish.
func (w *Worker[D, R]) Pause() {
	if !w.paused.CompareAndSwap(false, true) {
		return
	}

	// Abort an in-flight blocking read so a paused worker does not pull a
	// new job into active (upstream disconnects the blocking connection).
	w.abortWait()
	w.emit("paused")
}

// PauseAndWait pauses the worker and blocks until all jobs in progress have
// finished or ctx is done, like upstream worker.pause(). It returns ctx.Err()
// when the wait is cut short.
func (w *Worker[D, R]) PauseAndWait(ctx context.Context) error {
	w.Pause()
	for {
		w.jobsInProgress.Lock()
		n := len(w.jobsInProgress.jobs)
		w.jobsInProgress.Unlock()
		if n == 0 || w.closing.Load() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Resume resumes processing of this worker (if paused).
func (w *Worker[D, R]) Resume() {
	if !w.paused.Load() {
		return
	}

	w.paused.Store(false)
	select {
	case w.pauseCh <- struct{}{}:
	default:
	}
	w.emit("resumed")
}

// IsPaused returns true if the worker is paused.
func (w *Worker[D, R]) IsPaused() bool {
	return w.paused.Load()
}

// IsRunning returns true if the worker is running.
func (w *Worker[D, R]) IsRunning() bool {
	return w.running.Load()
}

// Ping checks the connection to the Redis server.
func (w *Worker[D, R]) Ping(ctx context.Context) error {
	_, err := w.redisClient.Ping(ctx).Result()
	return err
}

// Wait waits for the worker's main goroutine to finish.
// Returns immediately if Run() has not been called.
func (w *Worker[D, R]) Wait() {
	if !w.running.Load() {
		return
	}
	w.wg.Wait()
}

// Close closes the worker immediately: in-flight jobs are abandoned (their
// process contexts are cancelled) and no waiting is done, like upstream
// worker.close(true). Use Shutdown for a graceful, bounded close.
// Close is idempotent.
func (w *Worker[D, R]) Close() error {
	if !w.closing.CompareAndSwap(false, true) {
		return nil
	}
	w.forceClosing.Store(true)

	w.abortWait()

	w.mutex.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	w.stopTimers()
	w.mutex.Unlock()
	w.wg.Wait()

	// Release the fifoqueue's pool goroutines; without this every closed
	// worker would leak Concurrency parked goroutines.
	if w.asyncFifoQueue != nil {
		_ = w.asyncFifoQueue.WaitAll(minQueueCleanupTimeout)
	}

	w.running.Store(false)
	// Emitted once close completes, like upstream worker.close()'s finally.
	w.emit("closed")
	return nil
}

// Shutdown closes the worker gracefully: it stops fetching new jobs, then
// waits for the jobs in progress to finish before cancelling the worker's
// run context, bounded by ctx. When ctx is done first, the remaining jobs
// are abandoned and the returned error wraps ErrShutdownTimeout and
// ctx.Err(). Shutdown is idempotent.
func (w *Worker[D, R]) Shutdown(ctx context.Context) error {
	if !w.closing.CompareAndSwap(false, true) {
		return nil
	}
	// Emitted once close completes, like upstream worker.close()'s finally.
	defer w.emit("closed")

	// closing stops new fetches; abort the in-flight blocking read too.
	w.abortWait()

	// Drain the complete owned-job and FIFO lifecycle before cancelling runCtx
	// so fetched, queued, processing, and finalizing jobs all settle.
	var drainErr error
	for {
		w.jobsInProgress.Lock()
		n := len(w.jobsInProgress.jobs)
		w.jobsInProgress.Unlock()
		queued := 0
		if w.asyncFifoQueue != nil {
			queued = w.asyncFifoQueue.NumTotal()
		}
		if n == 0 && queued == 0 {
			break
		}
		select {
		case <-ctx.Done():
			drainErr = fmt.Errorf("%w: %d owned job(s), %d queued task(s): %w", ErrShutdownTimeout, n, queued, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
		if drainErr != nil {
			break
		}
	}
	if drainErr != nil {
		w.forceClosing.Store(true)
	} else {
		// The coordinator exits once closing is set and the FIFO is empty.
		w.wg.Wait()
	}

	w.mutex.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	w.stopTimers()
	w.mutex.Unlock()
	if drainErr != nil {
		w.wg.Wait()
	}

	var queueErr error
	if w.asyncFifoQueue != nil {
		if err := w.asyncFifoQueue.WaitAll(minQueueCleanupTimeout); err != nil {
			queueErr = fmt.Errorf("failed to close FIFO queue: %w", err)
		}
	}

	w.running.Store(false)
	return errors.Join(drainErr, queueErr)
}

// stopTimers stops the stalled-check and lock-extender timers, if running.
func (w *Worker[D, R]) stopTimers() {
	if w.stalledCheckTimer != nil {
		w.stalledCheckTimer.Stop()
	}
	if w.extendLocksTimer != nil {
		w.extendLocksTimer.Stop()
	}
}

// startStalledCheckTimer runs a stalled check immediately (upstream checks
// on start, so a restarted worker recovers stalled jobs right away), then
// reschedules itself every StalledInterval.
func (w *Worker[D, R]) startStalledCheckTimer() {
	if w.closing.Load() || w.opts.SkipStalledCheck {
		return
	}
	if err := w.moveStalledJobsToWait(); err != nil {
		w.emit("error", err)
	}
	// The timer field is read by stopTimers under w.mutex; write it under
	// the same mutex (this callback runs on a timer goroutine).
	w.mutex.Lock()
	if !w.closing.Load() {
		w.stalledCheckTimer = time.AfterFunc(w.opts.StalledInterval, func() {
			w.startStalledCheckTimer()
		})
	}
	w.mutex.Unlock()
}

// startLockExtender starts the lock extender.
func (w *Worker[D, R]) startLockExtender() {
	if w.forceClosing.Load() || w.opts.SkipLockRenewal || w.runCtx == nil {
		return
	}
	select {
	case <-w.runCtx.Done():
		return
	default:
	}
	w.mutex.Lock()
	w.extendLocksTimer = time.AfterFunc(w.opts.LockRenewTime/2, func() {
		if w.forceClosing.Load() || w.opts.SkipLockRenewal {
			return
		}
		select {
		case <-w.runCtx.Done():
			return
		default:
		}
		// Collect under the lock, extend after releasing it: extending emits
		// error events, and a listener calling PauseAndWait/Shutdown (which
		// poll jobsInProgress) must not deadlock against this goroutine.
		now := time.Now()
		var jobsToExtend []*rawJob
		w.jobsInProgress.Lock()
		for id, jp := range w.jobsInProgress.jobs {
			if jp.ts.IsZero() {
				jp.ts = now
				w.jobsInProgress.jobs[id] = jp
				continue
			}
			if jp.ts.Add(w.opts.LockRenewTime / 2).Before(now) {
				jp.ts = now
				w.jobsInProgress.jobs[id] = jp
				job := jp.job
				jobsToExtend = append(jobsToExtend, &job)
			}
		}
		w.jobsInProgress.Unlock()
		if len(jobsToExtend) > 0 {
			if err := w.extendLocksForJobs(jobsToExtend); err != nil {
				w.emit("error", err)
			}
		}
		w.startLockExtender()
	})
	w.mutex.Unlock()
}

// retryIfFailed retries a job if it failed, respecting context cancellation.
func (w *Worker[D, R]) retryIfFailed(jobFunc func() (*rawJob, error), delay time.Duration) (rawJob, error) {
	for {
		select {
		case <-w.runCtx.Done():
			return rawJob{}, w.runCtx.Err()
		default:
		}

		nextJob, err := jobFunc()
		if err != nil {
			w.emit("error", err)
			if delay > 0 {
				if err := sleepContext(w.runCtx, delay); err != nil {
					return rawJob{}, err
				}
				continue
			}
			return rawJob{}, err
		}
		if nextJob == nil {
			// No job available: return immediately like upstream; the
			// blocking wait in getNextJob provides the pacing.
			return rawJob{}, nil
		}
		return *nextJob, nil
	}
}

// extendLocksForJobs extends the locks for the given jobs.
// It continues extending all jobs even if some fail, returning all errors joined.
func (w *Worker[D, R]) extendLocksForJobs(jobs []*rawJob) error {
	var errs []error
	for _, job := range jobs {
		keys := []string{w.keyPrefix + job.id + ":lock", w.keyPrefix + "stalled"}
		result, err := lua.ExtendLock(w.runCtx, w.redisClient, keys, job.token, int(w.opts.LockDuration/time.Millisecond), job.id)
		if err == nil {
			if n, ok := result.(int64); ok && n == 0 {
				err = ErrMissingLock
			}
		}
		if err != nil {
			w.emit("error", fmt.Errorf("could not renew lock for job %s: %w", job.id, err))
			errs = append(errs, fmt.Errorf("job %s: %w", job.id, err))
		}
	}
	return errors.Join(errs...)
}

// moveStalledJobsToWait moves stalled jobs to the wait list.
func (w *Worker[D, R]) moveStalledJobsToWait() error {
	chunkSize := 50
	parentData, err := w.loadStalledParentData()
	if err != nil {
		return err
	}
	timestamp := time.Now().UnixMilli()
	failed, stalled, err := func() (failed []string, stalled []string, err error) {
		keys := []string{w.keyPrefix + "stalled", w.keyPrefix + "wait", w.keyPrefix + "active", w.keyPrefix + "failed", w.keyPrefix + "stalled-check", w.keyPrefix + "meta", w.keyPrefix + "paused", w.keyPrefix + "events"}
		result, err := lua.MoveStalledJobsToWait(w.runCtx, w.redisClient, keys, w.opts.MaxStalledCount, w.keyPrefix, timestamp, int(w.opts.StalledInterval/time.Millisecond))
		if err != nil {
			return nil, nil, err
		}
		resultSlice, ok := result.([]any)
		if !ok || len(resultSlice) != 2 {
			return nil, nil, fmt.Errorf("unexpected Lua script result format")
		}

		failedInterfaces, ok := resultSlice[0].([]any)
		if !ok {
			return nil, nil, fmt.Errorf("failed jobs format incorrect")
		}

		failed = make([]string, len(failedInterfaces))
		for i, f := range failedInterfaces {
			failed[i], ok = f.(string)
			if !ok {
				return nil, nil, fmt.Errorf("failed job ID format incorrect")
			}
		}

		stalledInterfaces, ok := resultSlice[1].([]any)
		if !ok {
			return nil, nil, fmt.Errorf("stalled jobs format incorrect")
		}

		stalled = make([]string, len(stalledInterfaces))
		for i, s := range stalledInterfaces {
			stalled[i], ok = s.(string)
			if !ok {
				return nil, nil, fmt.Errorf("stalled job ID format incorrect")
			}
		}

		return failed, stalled, nil
	}()
	if err != nil {
		return err
	}

	for _, jobId := range stalled {
		w.emit("stalled", jobId, "active")
	}
	for _, jobID := range failed {
		data := parentData[jobID]
		if err := lua.HandleStalledParent(w.runCtx, w.redisClient, w.keyPrefix+jobID, timestamp, data.opts, data.parentKey, data.parent); err != nil {
			return err
		}
	}

	failedJobs := make([]rawJob, 0, len(failed))
	for i, jobId := range failed {
		j, err := jobFromId(w.runCtx, w.redisClient, w.keyPrefix, jobId)
		if err != nil {
			if errors.Is(err, ErrJobNotFound) {
				continue
			}
			return err
		}

		failedJobs = append(failedJobs, j)

		if (i+1)%chunkSize == 0 {
			w.notifyFailedJobs(failedJobs)
			failedJobs = failedJobs[:0]
		}
	}

	w.notifyFailedJobs(failedJobs)
	return nil
}

type stalledParentData struct {
	opts      string
	parentKey string
	parent    string
}

func (w *Worker[D, R]) loadStalledParentData() (map[string]stalledParentData, error) {
	jobIDs, err := w.redisClient.SMembers(w.runCtx, w.keyPrefix+"stalled").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read stalled job metadata: %w", err)
	}
	data := make(map[string]stalledParentData, len(jobIDs))
	if len(jobIDs) == 0 {
		return data, nil
	}
	pipe := w.redisClient.Pipeline()
	commands := make(map[string]*redis.SliceCmd, len(jobIDs))
	for _, jobID := range jobIDs {
		commands[jobID] = pipe.HMGet(w.runCtx, w.keyPrefix+jobID, "opts", "parentKey", "parent")
	}
	if _, err := pipe.Exec(w.runCtx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("failed to load stalled job metadata: %w", err)
	}
	for jobID, command := range commands {
		values := command.Val()
		entry := stalledParentData{}
		if len(values) > 0 && values[0] != nil {
			entry.opts, _ = values[0].(string)
		}
		if len(values) > 1 && values[1] != nil {
			entry.parentKey, _ = values[1].(string)
		}
		if len(values) > 2 && values[2] != nil {
			entry.parent, _ = values[2].(string)
		}
		data[jobID] = entry
	}
	return data, nil
}

// notifyFailedJobs emits a failed event for each job in the provided list.
func (w *Worker[D, R]) notifyFailedJobs(jobs []rawJob) {
	for _, job := range jobs {
		w.emit("failed", job, errors.New("job stalled more than allowable limit"), "active")
	}
}

// clampMin returns v if v >= min, otherwise min.
func clampMin(v, min int64) int64 {
	if v < min {
		return min
	}
	return v
}
