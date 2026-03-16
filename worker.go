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
	name        string
	token       uuid.UUID
	ee          *eventemitter.EventEmitter
	running     atomic.Bool
	closing     atomic.Bool
	paused      atomic.Bool
	redisClient redis.Cmdable
	ctx         context.Context
	cancel      context.CancelFunc
	prefix      string
	keyPrefix   string
	mutex       sync.Mutex
	wg          sync.WaitGroup
	opts        WorkerOptions
	processFn   ProcessFunc[D, R]

	extendLocksTimer  *time.Timer
	stalledCheckTimer *time.Timer

	jobsInProgress *jobsInProgress

	blockUntil atomic.Int64
	limitUntil atomic.Int64
	drained    atomic.Bool

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
	ShutdownTimeout  time.Duration
}

// BackoffStrategyFunc is a custom function to calculate backoff delay in milliseconds.
// Return 0 for immediate retry, or a positive value for the delay.
// Return -1 to signal "do not retry" (move to failed).
type BackoffStrategyFunc func(attemptsMade int, opts *BackoffOptions, err error, jobID string) int

type RateLimiterOptions struct {
	Max      int `msgpack:"max"`
	Duration int `msgpack:"duration"`
}

type MetricsOptions struct {
	MaxDataPoints int
}

type GetNextJobOptions struct {
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
func (w *Worker[D, R]) OnCompleted(fn func(job *Job[D], result R)) eventemitter.ListenerID {
	return w.ee.On("completed", func(args ...interface{}) {
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
func (w *Worker[D, R]) OnFailed(fn func(job *Job[D], err error)) eventemitter.ListenerID {
	return w.ee.On("failed", func(args ...interface{}) {
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
func (w *Worker[D, R]) OnActive(fn func(job *Job[D])) eventemitter.ListenerID {
	return w.ee.On("active", func(args ...interface{}) {
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
func (w *Worker[D, R]) OnStalled(fn func(jobID string)) eventemitter.ListenerID {
	return w.ee.On("stalled", func(args ...interface{}) {
		if len(args) >= 1 {
			if id, ok := args[0].(string); ok {
				fn(id)
			}
		}
	})
}

// OnDrained registers a typed callback for when the queue is drained and returns a ListenerID.
func (w *Worker[D, R]) OnDrained(fn func()) eventemitter.ListenerID {
	return w.ee.On("drained", func(args ...interface{}) {
		fn()
	})
}

// OnError registers a typed callback for worker errors and returns a ListenerID.
func (w *Worker[D, R]) OnError(fn func(err error)) eventemitter.ListenerID {
	return w.ee.On("error", func(args ...interface{}) {
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
func (w *Worker[D, R]) OnRetriesExhausted(fn func(job *Job[D], err error)) eventemitter.ListenerID {
	return w.ee.On("retries-exhausted", func(args ...interface{}) {
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
	JobData    map[string]interface{}
	ID         string
	LimitUntil int64
	DelayUntil int64
}

// Default timing configuration
const (
	defaultLockDuration    = 30 * time.Second
	defaultLockRenewTime   = 15 * time.Second
	defaultStalledInterval = 30 * time.Second
	defaultRunRetryDelay   = 250 * time.Millisecond
	defaultDrainDelay      = 1 * time.Second
	defaultShutdownTimeout = 5 * time.Second
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
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.RunRetryDelay <= 0 {
		opts.RunRetryDelay = defaultRunRetryDelay
	}
	if opts.DrainDelay < 0 {
		opts.DrainDelay = defaultDrainDelay
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = defaultShutdownTimeout
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

	return w, nil
}

// Emit emits the event with the given name and arguments.
func (w *Worker[D, R]) Emit(event string, args ...interface{}) {
	w.ee.Emit(event, args...)
}

// Off removes a specific listener by its ListenerID.
func (w *Worker[D, R]) Off(event string, id eventemitter.ListenerID) {
	w.ee.RemoveListener(event, id)
}

// On listens for the event and returns a ListenerID that can be used with Off.
func (w *Worker[D, R]) On(event string, listener func(...interface{})) eventemitter.ListenerID {
	return w.ee.On(event, listener)
}

// Once listens for the event only once and returns a ListenerID.
func (w *Worker[D, R]) Once(event string, listener func(...interface{})) eventemitter.ListenerID {
	return w.ee.Once(event, listener)
}

// createJob constructs a rawJob from the map data retrieved from Redis.
func (w *Worker[D, R]) createJob(jobData map[string]interface{}, jobId string) (rawJob, error) {
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

	w.running.Store(true)

	if w.closing.Load() {
		return ErrWorkerClosing
	}

	ctx, cancel := context.WithCancel(ctx)
	w.ctx = ctx
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
		if w.closing.Load() || w.paused.Load() {
			return
		}
		tokenPostfix++
		token := fmt.Sprintf("%s:%d", w.token, tokenPostfix)
		if err := w.asyncFifoQueue.Add(func() (rawJob, error) {
			j, err := w.retryIfFailed(func() (*rawJob, error) {
				nextJob, err := w.getNextJob(token, GetNextJobOptions{Block: true})
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
				w.Emit("error", fmt.Sprintf("Error fetching job: %v", err))
				return rawJob{}, err
			}

			if j.id == "" {
				return rawJob{}, nil
			}

			return j, nil
		}); err != nil {
			w.Emit("error", fmt.Sprintf("Error adding fetch task to queue: %v", err))
		}
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for i := 0; i < w.opts.Concurrency; i++ {
			addFetchTask()
		}

		for {
			select {
			case <-w.ctx.Done():
				w.Emit("closing")
				return
			default:
			}

			jobResult, taskErr := w.asyncFifoQueue.Fetch(w.ctx)

			if taskErr != nil {
				if errors.Is(taskErr, context.Canceled) || errors.Is(taskErr, fifoqueue.ErrQueueClosed) {
					w.Emit("info", fmt.Sprintf("Worker stopping due to context cancellation or queue closure: %v", taskErr))
					return
				}
				w.Emit("error", fmt.Sprintf("Error fetching task result from queue: %v", taskErr))
				addFetchTask()
				continue
			}

			if jobResult != nil && jobResult.id != "" && jobResult.id != "0" {
				fetchedJob := *jobResult
				token := fetchedJob.token

				if err := w.asyncFifoQueue.Add(func() (rawJob, error) {
					w.processJob(
						fetchedJob,
						token,
						func() bool {
							return w.asyncFifoQueue.NumTotal() < w.opts.Concurrency
						},
					)
					return rawJob{}, nil
				}); err != nil {
					w.Emit("error", fmt.Sprintf("Error adding process task for job %s: %v", fetchedJob.id, err))
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
func (w *Worker[D, R]) getNextJob(token string, opts GetNextJobOptions) (*rawJob, error) {
	if w.paused.Load() {
		if opts.Block {
			for w.paused.Load() && !w.closing.Load() {
				select {
				case <-w.pauseCh:
				case <-time.After(100 * time.Millisecond):
				case <-w.ctx.Done():
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

	if w.drained.Load() && opts.Block && w.limitUntil.Load() == 0 {
		jobID, err := w.waitForJob()
		if err != nil {
			if !w.paused.Load() && !w.closing.Load() {
				return nil, fmt.Errorf("failed to wait for job: %w", err)
			}
			return nil, nil
		}

		if jobID == "" {
			return nil, nil
		}

		return w.moveToActive(token, jobID)
	}

	if limitUntil := w.limitUntil.Load(); limitUntil != 0 {
		if err := w.delay(limitUntil); err != nil {
			return nil, fmt.Errorf("failed to delay: %w", err)
		}
	}
	return w.moveToActive(token, "")
}

// moveToActive moves the job to the active list.
func (w *Worker[D, R]) moveToActive(token string, jobId string) (*rawJob, error) {
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

	opts := map[string]interface{}{
		"token":        token,
		"lockDuration": int(w.opts.LockDuration / time.Millisecond),
	}
	if w.opts.Limiter != nil {
		opts["limiter"] = map[string]interface{}{
			"max":      w.opts.Limiter.Max,
			"duration": w.opts.Limiter.Duration,
		}
	}

	msgPackedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal opts: %w", err)
	}

	rawResult, err := lua.MoveToActive(w.ctx, w.redisClient, keys, w.keyPrefix, time.Now().UnixMilli(), jobId, string(msgPackedOpts))
	if err != nil {
		return nil, err
	}

	rawResultSlice, ok := rawResult.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type for rawResult: %T", rawResult)
	}

	result := raw2NextJobData(rawResultSlice)
	if result == nil {
		w.Emit("error", fmt.Sprintf("moveToActive received invalid data from Lua for token %s, jobID %s", token, jobId))
		return nil, nil
	}

	if result.ID == "0" || result.ID == "" {
		return nil, nil
	}

	return w.nextJobFromJobData(result.JobData, result.ID, result.LimitUntil, result.DelayUntil, token)
}

// raw2NextJobData processes raw data and returns a typed nextJobData structure.
func raw2NextJobData(raw []interface{}) *nextJobData {
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

// waitForJob waits for a job ID from the wait list.
func (w *Worker[D, R]) waitForJob() (string, error) {
	blockTimeout := w.opts.DrainDelay
	if blockTimeout <= 0 {
		blockTimeout = 10 * time.Millisecond
	}
	if blockUntil := w.blockUntil.Load(); blockUntil > 0 {
		remaining := time.Until(time.UnixMilli(blockUntil))
		if remaining < 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		blockTimeout = remaining
	}
	if blockTimeout > 10*time.Second {
		blockTimeout = 10 * time.Second
	}
	result, err := w.redisClient.BLMove(w.ctx, w.keyPrefix+"wait", w.keyPrefix+"active", "LEFT", "RIGHT", blockTimeout).Result()
	if err != nil {
		return "", err
	}
	return result, nil
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
func (w *Worker[D, R]) delay(until int64) error {
	now := time.Now().UnixMilli()
	if until > now {
		return sleepContext(w.ctx, time.Duration(until-now)*time.Millisecond)
	}
	return nil
}

// nextJobFromJobData processes the next job data and returns a rawJob.
func (w *Worker[D, R]) nextJobFromJobData(
	jobData map[string]interface{},
	jobID string,
	limitUntil int64,
	delayUntil int64,
	token string,
) (*rawJob, error) {
	if jobData == nil {
		if !w.drained.Load() {
			w.Emit("drained")
			w.drained.Store(true)
			w.blockUntil.Store(0)
		}
	}

	w.limitUntil.Store(clampMin(limitUntil, 0))
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
	if job.opts.Repeat != nil && (job.opts.Repeat.Every != 0 || job.opts.Repeat.Pattern != "") {
		// TODO: Repeatable.AddNextRepeatableJob
	}
	return &job, nil
}

// processJob processes the job.
func (w *Worker[D, R]) processJob(job rawJob, token string, fetchNextCallback func() bool) (rawJob, error) {
	if w.closing.Load() || w.paused.Load() {
		return rawJob{}, nil
	}

	w.Emit("active", job, "waiting")

	w.jobsInProgress.Lock()
	w.jobsInProgress.jobs[job.id] = jobInProgress{job: job, ts: time.Now()}
	w.jobsInProgress.Unlock()

	var result R
	var err error

	processFnCtx, processFnCtxCancel := context.WithCancel(w.ctx)
	defer processFnCtxCancel()

	func() {
		defer func() {
			if r := recover(); r != nil {
				w.Emit("error", fmt.Sprintf("Panic recovered for job %s with token %s: %v", job.id, token, r))
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

	w.jobsInProgress.Lock()
	delete(w.jobsInProgress.jobs, job.id)
	w.jobsInProgress.Unlock()

	if err != nil {
		return w.handleJobError(job, token, err, fetchNextCallback)
	}

	return w.handleJobSuccess(job, token, result, fetchNextCallback)
}

// handleJobError handles the error path of processJob.
func (w *Worker[D, R]) handleJobError(job rawJob, token string, err error, fetchNextCallback func() bool) (rawJob, error) {
	if errors.Is(err, ErrRateLimit) {
		if w.scripts != nil {
			pttl, mErr := w.scripts.moveJobFromActiveToWait(w.ctx, job.id, token)
			if mErr != nil {
				w.Emit("error", fmt.Sprintf("moveJobFromActiveToWait failed for %s: %v", job.id, mErr))
			} else {
				w.limitUntil.Store(time.Now().Add(time.Duration(pttl) * time.Millisecond).UnixMilli())
			}
		}
		return rawJob{}, err
	}

	if errors.Is(err, ErrDelayed) || errors.Is(err, ErrWaitingChildren) {
		return rawJob{}, err
	}

	shouldMoveToFailed := false

	if job.attemptsMade < job.opts.Attempts {
		delayMs := 0
		if w.opts.BackoffStrategy != nil {
			delayMs = w.opts.BackoffStrategy(job.attemptsMade, job.opts.Backoff, err, job.id)
		} else if job.opts.Backoff != nil {
			delayMs = backoffutil.Calculate(backoffutil.Options{Type: job.opts.Backoff.Type, Delay: job.opts.Backoff.Delay}, job.attemptsMade)
		} else if w.opts.Backoff != nil {
			delayMs = backoffutil.Calculate(backoffutil.Options{Type: w.opts.Backoff.Type, Delay: w.opts.Backoff.Delay}, job.attemptsMade)
		}

		if delayMs > 0 {
			keys, args := w.scripts.moveToDelayedArgs(job.id, time.Now().UnixMilli()+int64(delayMs), token)
			if _, derr := lua.MoveToDelayed(w.ctx, w.redisClient, keys, args...); derr != nil {
				w.Emit("error", fmt.Sprintf("moveToDelayed failed for %s: %v", job.id, derr))
				keysR, argsR := w.scripts.retryJobArgs(job.id, job.opts.Lifo, token)
				if _, rerr := lua.RetryJob(w.ctx, w.redisClient, keysR, argsR...); rerr != nil {
					w.Emit("error", fmt.Sprintf("retryJob failed for %s: %v", job.id, rerr))
					shouldMoveToFailed = true
				} else {
					w.Emit("failed", job, err, "active")
					return rawJob{}, err
				}
			} else {
				w.Emit("failed", job, err, "active")
				return rawJob{}, err
			}
		} else if delayMs == 0 {
			keys, args := w.scripts.retryJobArgs(job.id, job.opts.Lifo, token)
			if _, rerr := lua.RetryJob(w.ctx, w.redisClient, keys, args...); rerr != nil {
				w.Emit("error", fmt.Sprintf("retryJob failed for %s: %v", job.id, rerr))
				shouldMoveToFailed = true
			} else {
				w.Emit("failed", job, err, "active")
				return rawJob{}, err
			}
		} else {
			shouldMoveToFailed = true
		}
	} else {
		shouldMoveToFailed = true
	}

	if shouldMoveToFailed {
		if job.opts.Attempts > 0 && job.attemptsMade >= job.opts.Attempts {
			w.Emit("retries-exhausted", job, err)
		}
		var removeOnFail KeepJobs
		if w.opts.RemoveOnFail != nil {
			removeOnFail = *w.opts.RemoveOnFail
		}
		lockDurationMs := int(w.opts.LockDuration / time.Millisecond)
		maxMetricsSize := ""
		if w.opts.Metrics != nil && w.opts.Metrics.MaxDataPoints > 0 {
			maxMetricsSize = strconv.Itoa(w.opts.Metrics.MaxDataPoints)
		}
		if moveErr := jobMoveToFailed(w.ctx, w.scripts, &job, err, token, removeOnFail, fetchNextCallback(), lockDurationMs, maxMetricsSize); moveErr != nil {
			w.Emit("error", fmt.Sprintf("Error explicitly moving job %s to failed: %v", job.id, moveErr))
		}
		w.Emit("failed", job, err, "active")
		return rawJob{}, err
	}

	return rawJob{}, err
}

// handleJobSuccess handles the success path of processJob.
func (w *Worker[D, R]) handleJobSuccess(job rawJob, token string, result R, fetchNextCallback func() bool) (rawJob, error) {
	// Check if this is a repeatable job and schedule the next one
	if job.opts.Repeat != nil {
		jobJSONData, ok := job.data.(string)
		if !ok {
			w.Emit("error", fmt.Sprintf("Repeatable job %s has non-string data (%T), cannot reschedule", job.id, job.data))
		} else {
			if scheduleErr := scheduleNextRepeatableJobInternal(w.ctx, w.redisClient, w.keyPrefix, job.name, jobJSONData, job.opts); scheduleErr != nil {
				w.Emit("error", fmt.Sprintf("Failed to schedule next instance for repeatable job %s: %v", job.id, scheduleErr))
			}
		}
	}

	lockDurationMs := int(w.opts.LockDuration / time.Millisecond)
	maxMetricsSize := ""
	if w.opts.Metrics != nil && w.opts.Metrics.MaxDataPoints > 0 {
		maxMetricsSize = strconv.Itoa(w.opts.Metrics.MaxDataPoints)
	}

	job.returnValue = result
	stringifiedReturnValue, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		w.Emit("error", fmt.Sprintf("Error marshaling result for job %s: %v", job.id, marshalErr))
		return rawJob{}, marshalErr
	}

	getNext := fetchNextCallback() && !(w.closing.Load() || w.paused.Load())
	keys, args, scriptErr := w.scripts.moveToFinishedArgs(&job, string(stringifiedReturnValue), "returnvalue", job.opts.RemoveOnComplete, "completed", token, time.Now(), getNext, lockDurationMs, maxMetricsSize)
	if scriptErr != nil {
		w.Emit("error", fmt.Sprintf("Error moving job to completed: %v", scriptErr))
		return rawJob{}, scriptErr
	}

	job.finishedOn = time.Now()
	rawLuaResult, luaErr := lua.MoveToFinished(w.ctx, w.redisClient, keys, args...)
	if luaErr != nil {
		w.Emit("error", fmt.Sprintf("Error moving job to completed: %v", luaErr))
		return rawJob{}, luaErr
	}

	var completed []interface{}
	switch v := rawLuaResult.(type) {
	case int64:
		completed = []interface{}{v}
	case []interface{}:
		completed = v
	default:
		return rawJob{}, fmt.Errorf("unexpected type for rawResult: %T", rawLuaResult)
	}

	if code, ok := completed[0].(int64); ok && code < 0 {
		switch code {
		case -1:
			return rawJob{}, fmt.Errorf("missing key for job %s: %d", job.id, completed)
		case -2:
			return rawJob{}, fmt.Errorf("missing lock for job %s: %d", job.id, completed)
		case -3:
			return rawJob{}, fmt.Errorf("not in active set for job %s: %d", job.id, completed)
		case -4:
			return rawJob{}, fmt.Errorf("has pending dependencies for job %s: %d", job.id, completed)
		case -6:
			return rawJob{}, fmt.Errorf("lock is not owned by this client for job %s: %d", job.id, completed)
		default:
			return rawJob{}, fmt.Errorf("unknown error for job %s: %d", job.id, completed)
		}
	}

	w.Emit("completed", job, result, "active")

	nextData := raw2NextJobData(completed)
	if nextData != nil {
		j, nextErr := w.nextJobFromJobData(nextData.JobData, nextData.ID, nextData.LimitUntil, nextData.DelayUntil, token)
		if nextErr != nil {
			w.Emit("error", fmt.Sprintf("Error getting next job: %v", nextErr))
			return rawJob{}, nextErr
		}
		if j != nil {
			return *j, nil
		}
	}
	return rawJob{}, nil
}

// Pause pauses processing of this worker.
func (w *Worker[D, R]) Pause() {
	if w.paused.Load() {
		return
	}

	w.paused.Store(true)
	w.Emit("paused")
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
	w.Emit("resumed")
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

// Close closes the worker gracefully with timeout.
func (w *Worker[D, R]) Close() error {
	w.mutex.Lock()

	if w.closing.Load() {
		w.mutex.Unlock()
		return nil
	}

	w.closing.Store(true)

	if w.cancel != nil {
		w.cancel()
	}

	if w.stalledCheckTimer != nil {
		w.stalledCheckTimer.Stop()
	}
	if w.extendLocksTimer != nil {
		w.extendLocksTimer.Stop()
	}

	timeout := w.opts.ShutdownTimeout
	w.mutex.Unlock()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	var timedOut bool
	select {
	case <-done:
	case <-time.After(timeout):
		w.Emit("error", fmt.Errorf("worker shutdown timed out after %v", timeout))
		timedOut = true
	}

	var queueErr error
	if w.asyncFifoQueue != nil {
		queueTimeout := timeout / 2
		if queueTimeout < minQueueCleanupTimeout {
			queueTimeout = minQueueCleanupTimeout
		}
		if err := w.asyncFifoQueue.WaitAll(queueTimeout); err != nil {
			queueErr = fmt.Errorf("failed to close FIFO queue: %w", err)
		}
	}

	if timedOut && queueErr != nil {
		return errors.Join(fmt.Errorf("worker shutdown timed out after %v", timeout), queueErr)
	}
	if timedOut {
		return fmt.Errorf("worker shutdown timed out after %v", timeout)
	}
	if queueErr != nil {
		return queueErr
	}
	return nil
}

// startStalledCheckTimer starts the stalled check timer.
func (w *Worker[D, R]) startStalledCheckTimer() {
	if w.closing.Load() || w.opts.SkipStalledCheck {
		return
	}
	w.stalledCheckTimer = time.AfterFunc(w.opts.StalledInterval, func() {
		if w.closing.Load() || w.opts.SkipStalledCheck {
			return
		}
		if err := w.moveStalledJobsToWait(); err != nil {
			w.Emit("error", err)
		}
		w.startStalledCheckTimer()
	})
}

// startLockExtender starts the lock extender.
func (w *Worker[D, R]) startLockExtender() {
	if w.closing.Load() || w.opts.SkipLockRenewal {
		return
	}
	w.extendLocksTimer = time.AfterFunc(w.opts.LockRenewTime/2, func() {
		w.jobsInProgress.Lock()
		defer w.jobsInProgress.Unlock()
		if w.closing.Load() || w.opts.SkipLockRenewal {
			return
		}
		now := time.Now()
		var jobsToExtend []*rawJob
		for id, jp := range w.jobsInProgress.jobs {
			if jp.ts.IsZero() {
				jp.ts = now
				w.jobsInProgress.jobs[id] = jp
				continue
			}
			if jp.ts.Add(w.opts.LockRenewTime / 2).Before(now) {
				jp.ts = now
				w.jobsInProgress.jobs[id] = jp
				jobsToExtend = append(jobsToExtend, &jp.job)
			}
		}
		if len(jobsToExtend) > 0 {
			if err := w.extendLocksForJobs(jobsToExtend); err != nil {
				w.Emit("error", err)
			}
		}
		w.startLockExtender()
	})
}

// retryIfFailed retries a job if it failed, respecting context cancellation.
func (w *Worker[D, R]) retryIfFailed(jobFunc func() (*rawJob, error), delay time.Duration) (rawJob, error) {
	for {
		select {
		case <-w.ctx.Done():
			return rawJob{}, w.ctx.Err()
		default:
		}

		nextJob, err := jobFunc()
		if err != nil {
			w.Emit("error", err)
			if delay > 0 {
				if err := sleepContext(w.ctx, delay); err != nil {
					return rawJob{}, err
				}
				continue
			}
			return rawJob{}, err
		}
		if nextJob == nil {
			if delay > 0 {
				if err := sleepContext(w.ctx, delay); err != nil {
					return rawJob{}, err
				}
				continue
			}
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
		keys := []string{w.keyPrefix + "lock", w.keyPrefix + "stalled"}
		_, err := lua.ExtendLock(w.ctx, w.redisClient, keys, job.token, int(w.opts.LockDuration/time.Millisecond), job.id)
		if err != nil {
			w.Emit("error", fmt.Errorf("could not renew lock for job %s: %w", job.id, err))
			errs = append(errs, fmt.Errorf("job %s: %w", job.id, err))
		}
	}
	return errors.Join(errs...)
}

// moveStalledJobsToWait moves stalled jobs to the wait list.
func (w *Worker[D, R]) moveStalledJobsToWait() error {
	chunkSize := 50
	failed, stalled, err := func() (failed []string, stalled []string, error error) {
		keys := []string{w.keyPrefix + "stalled", w.keyPrefix + "wait", w.keyPrefix + "active", w.keyPrefix + "failed", w.keyPrefix + "stalled-check", w.keyPrefix + "meta", w.keyPrefix + "paused", w.keyPrefix + "events"}
		result, err := lua.MoveStalledJobsToWait(w.ctx, w.redisClient, keys, w.opts.MaxStalledCount, w.keyPrefix, time.Now().Unix(), int(w.opts.StalledInterval/time.Millisecond))
		if err != nil {
			return nil, nil, err
		}
		resultSlice, ok := result.([]interface{})
		if !ok || len(resultSlice) != 2 {
			return nil, nil, fmt.Errorf("unexpected Lua script result format")
		}

		failedInterfaces, ok := resultSlice[0].([]interface{})
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

		stalledInterfaces, ok := resultSlice[1].([]interface{})
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
		w.Emit("stalled", jobId, "active")
	}

	failedJobs := make([]rawJob, 0, len(failed))
	for i, jobId := range failed {
		j, err := jobFromId(w.ctx, w.redisClient, w.keyPrefix, jobId)
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

// notifyFailedJobs emits a failed event for each job in the provided list.
func (w *Worker[D, R]) notifyFailedJobs(jobs []rawJob) {
	for _, job := range jobs {
		w.Emit("failed", job, errors.New("job stalled more than allowable limit"), "active")
	}
}

// clampMin returns v if v >= min, otherwise min.
func clampMin(v, min int64) int64 {
	if v < min {
		return min
	}
	return v
}
