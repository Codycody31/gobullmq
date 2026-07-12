package gobullmq

import (
	"context"
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
	cr "github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
)

// Queue represents a job queue backed by Redis.
// D is the type of data stored in jobs created by this queue.
type Queue[D any] struct {
	ee        *eventemitter.EventEmitter
	name      string
	token     uuid.UUID
	keyPrefix string
	client    redis.Cmdable
	prefix    string
	closed    atomic.Bool

	streamEventsMaxLen int64
	defaultJobOptions  *JobOptions
	metaOnce           sync.Once
}

// QueueOptions holds configuration options for creating a new Queue.
type QueueOptions struct {
	Prefix             string
	StreamEventsMaxLen int64
	// DefaultJobOptions are merged under the per-call options of every Add
	// and AddBulk, like upstream defaultJobOptions.
	DefaultJobOptions *JobOptions
}

// BulkJob defines a job for AddBulk.
type BulkJob[D any] struct {
	Name string
	Data D
	// Opts overlays its non-zero fields on DefaultJobOptions.
	Opts JobOptions
	// Options are applied last and can express explicit zero-value overrides,
	// such as AddWithAttempts(0).
	Options []AddOption
}

func mergeBulkJobOptions(defaults *JobOptions, overrides JobOptions, options []AddOption) JobOptions {
	var merged JobOptions
	if defaults != nil {
		merged = *defaults
	}
	if overrides.Priority != 0 {
		merged.Priority = overrides.Priority
	}
	if overrides.RemoveOnComplete != nil {
		merged.RemoveOnComplete = overrides.RemoveOnComplete
	}
	if overrides.RemoveOnFail != nil {
		merged.RemoveOnFail = overrides.RemoveOnFail
	}
	if overrides.Attempts != 0 {
		merged.Attempts = overrides.Attempts
	}
	if overrides.Delay != 0 {
		merged.Delay = overrides.Delay
	}
	if overrides.Timestamp != 0 {
		merged.Timestamp = overrides.Timestamp
	}
	if overrides.Lifo {
		merged.Lifo = true
	}
	if overrides.JobID != "" {
		merged.JobID = overrides.JobID
	}
	if overrides.RepeatJobKey != "" {
		merged.RepeatJobKey = overrides.RepeatJobKey
	}
	if overrides.Repeat != nil {
		merged.Repeat = overrides.Repeat
	}
	if overrides.FailParentOnFailure {
		merged.FailParentOnFailure = true
	}
	if overrides.Parent != nil {
		merged.Parent = overrides.Parent
	}
	if overrides.RemoveDependencyOnFailure {
		merged.RemoveDependencyOnFailure = true
	}
	if overrides.KeepLogs != 0 {
		merged.KeepLogs = overrides.KeepLogs
	}
	if overrides.StackTraceLimit != 0 {
		merged.StackTraceLimit = overrides.StackTraceLimit
	}
	if overrides.SizeLimit != 0 {
		merged.SizeLimit = overrides.SizeLimit
	}
	if overrides.Backoff != nil {
		merged.Backoff = overrides.Backoff
	}
	if overrides.PrevMillis != 0 {
		merged.PrevMillis = overrides.PrevMillis
	}
	if overrides.WaitChildren {
		merged.WaitChildren = true
	}
	for _, option := range options {
		option(&merged)
	}
	return merged
}

// Name returns the queue name.
func (q *Queue[D]) Name() string {
	return q.name
}

// NewQueue creates a new Queue instance.
// The redis client is externally managed and will not be closed by the queue.
func NewQueue[D any](name string, client redis.Cmdable, opts *QueueOptions) (*Queue[D], error) {
	if name == "" {
		return nil, fmt.Errorf("queue name must be provided")
	}
	if client == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}

	prefix := "bull"
	streamEventsMaxLen := int64(10000)
	var defaultJobOptions *JobOptions
	if opts != nil {
		if opts.Prefix != "" {
			prefix = strings.Trim(opts.Prefix, ":")
			if prefix == "" {
				return nil, fmt.Errorf("prefix cannot be empty or just colons")
			}
		}
		if opts.StreamEventsMaxLen > 0 {
			streamEventsMaxLen = opts.StreamEventsMaxLen
		}
		defaultJobOptions = opts.DefaultJobOptions
	}

	q := &Queue[D]{
		name:               name,
		token:              uuid.New(),
		client:             client,
		prefix:             prefix,
		keyPrefix:          prefix + ":" + name + ":",
		streamEventsMaxLen: streamEventsMaxLen,
		defaultJobOptions:  defaultJobOptions,
	}

	q.ee = eventemitter.NewEventEmitter()

	return q, nil
}

// ListenerID identifies a listener registered with On, Once, or the typed
// On* helpers; pass it to Off to remove the listener.
type ListenerID = eventemitter.ListenerID

// emit emits the event with the given name and arguments.
func (q *Queue[D]) emit(event string, args ...any) {
	q.ee.Emit(event, args...)
}

// On listens for the event and returns a ListenerID that can be used with Off.
func (q *Queue[D]) On(event string, listener func(...any)) ListenerID {
	return q.ee.On(event, listener)
}

// Off removes a specific listener by its ListenerID.
func (q *Queue[D]) Off(event string, id ListenerID) {
	q.ee.RemoveListener(event, id)
}

// Once listens for the event only once and returns a ListenerID.
func (q *Queue[D]) Once(event string, listener func(...any)) ListenerID {
	return q.ee.Once(event, listener)
}

// OnWaiting registers a typed callback for when a job is added to the queue
// in the waiting state and returns a ListenerID.
func (q *Queue[D]) OnWaiting(fn func(job *Job[D])) ListenerID {
	return q.ee.On("waiting", func(args ...any) {
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

// OnPaused registers a typed callback for when the queue is paused and
// returns a ListenerID.
func (q *Queue[D]) OnPaused(fn func()) ListenerID {
	return q.ee.On("paused", func(...any) {
		fn()
	})
}

// OnResumed registers a typed callback for when the queue is resumed and
// returns a ListenerID.
func (q *Queue[D]) OnResumed(fn func()) ListenerID {
	return q.ee.On("resumed", func(...any) {
		fn()
	})
}

// OnCleaned registers a typed callback for when jobs are cleaned from the
// queue and returns a ListenerID.
func (q *Queue[D]) OnCleaned(fn func(jobIDs []string, state JobState)) ListenerID {
	return q.ee.On("cleaned", func(args ...any) {
		if len(args) >= 2 {
			ids, ok := args[0].([]string)
			if !ok {
				return
			}
			state, ok := args[1].(JobState)
			if !ok {
				return
			}
			fn(ids, state)
		}
	})
}

// OnError registers a typed callback for queue errors and returns a ListenerID.
func (q *Queue[D]) OnError(fn func(err error)) ListenerID {
	return q.ee.On("error", func(args ...any) {
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

// Add adds a new job to the queue.
// For repeatable jobs (AddWithRepeat), a nil job with nil error is returned
// when no iteration is due (repeat limit reached or endDate passed), matching
// upstream's undefined return.
func (q *Queue[D]) Add(ctx context.Context, jobName string, data D, addOpts ...AddOption) (*Job[D], error) {
	if q.closed.Load() {
		return nil, ErrQueueClosed
	}

	q.ensureMeta(ctx)

	// Start from the queue's default job options (merged under per-call
	// options, like upstream {...jobsOpts, ...opts}).
	base := JobOptions{}
	if q.defaultJobOptions != nil {
		base = *q.defaultJobOptions
	}
	opts := &base

	for _, fn := range addOpts {
		fn(opts)
	}

	jsonDataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal jobData to JSON: %w", err)
	}
	jsonData := string(jsonDataBytes)

	// Any non-nil repeat routes to the repeat path (upstream: any truthy
	// opts.repeat); malformed repeat opts then fail in getNextMillis, like
	// upstream's cron-parser throw. The jobId guard below applies only to
	// plain adds, as upstream's does.
	if opts.Repeat != nil {
		raw, err := addNextRepeatableJobInternal(ctx, q.client, q.keyPrefix, jobName, jsonData, *opts, true)
		if err != nil {
			return nil, fmt.Errorf("failed to add repeatable job: %w", err)
		}
		if raw == nil {
			// No iteration is due (limit reached or endDate passed).
			return nil, nil
		}
		q.emit("waiting", *raw)
		return &Job[D]{raw: raw, data: data}, nil
	}

	if opts.JobID != "" {
		// '0:' is reserved for delayed markers on the wait list.
		if opts.JobID == "0" || strings.HasPrefix(opts.JobID, "0:") {
			return nil, fmt.Errorf("jobId cannot be '0' or start with '0:'")
		}
	}

	raw, err := newJob(jobName, jsonData, *opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create new job: %w", err)
	}
	jobID, err := q.addJob(ctx, raw, opts.JobID)
	if err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	raw.id = jobID
	raw.setJobContext(q.client, q.keyPrefix)

	q.emit("waiting", raw)

	return &Job[D]{raw: &raw, data: data}, nil
}

// pause pauses or resumes the queue based on the provided flag.
func (q *Queue[D]) pause(ctx context.Context, doPause bool) error {
	p := "paused"
	src := "wait"
	dst := "paused"
	if !doPause {
		src = "paused"
		dst = "wait"
		p = "resumed"
	}

	keys := []string{
		q.toKey(src),
		q.toKey(dst),
		q.toKey("meta"),
		q.toKey("prioritized"),
		q.toKey("events"),
	}

	// The pause script returns no value; go-redis reports that as redis.Nil.
	_, err := lua.Pause(ctx, q.client, keys, p)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to pause or resume queue: %w", err)
	}

	return nil
}

// Pause pauses the queue, preventing new jobs from being processed.
func (q *Queue[D]) Pause(ctx context.Context) error {
	if err := q.pause(ctx, true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}
	q.emit("paused")
	return nil
}

// Resume resumes the queue, allowing jobs to be processed.
func (q *Queue[D]) Resume(ctx context.Context) error {
	if err := q.pause(ctx, false); err != nil {
		return fmt.Errorf("failed to resume queue: %w", err)
	}
	q.emit("resumed")
	return nil
}

// IsPaused checks if the queue is currently paused.
func (q *Queue[D]) IsPaused(ctx context.Context) (bool, error) {
	return q.client.HExists(ctx, q.keyPrefix+"meta", "paused").Result()
}

// addJob adds a job to the queue with the specified job ID.
func (q *Queue[D]) addJob(ctx context.Context, job rawJob, jobID string) (string, error) {
	return addJobInternal(ctx, q.client, q.keyPrefix, job, jobID)
}

// buildAddJobArgs prepares the keys and argv for the addJob script so callers
// can run it directly or inside a pipeline (flows).
func buildAddJobArgs(keyPrefix string, job rawJob, jobID string) ([]string, []any, error) {
	keys := []string{
		keyPrefix + "wait",
		keyPrefix + "paused",
		keyPrefix + "meta",
		keyPrefix + "id",
		keyPrefix + "delayed",
		keyPrefix + "prioritized",
		keyPrefix + "completed",
		keyPrefix + "events",
		keyPrefix + "pc",
	}

	// repeatJobKey must be packed as nil when unset; an empty string is not
	// nil in cmsgpack and would store an rjk field on every job.
	var repeatJobKey any
	if job.opts.RepeatJobKey != "" {
		repeatJobKey = job.opts.RepeatJobKey
	}

	args := []any{
		keyPrefix,
		jobID,
		job.name,
		job.timestamp,
		nil, // Parent Key
		nil, // Wait Children Key
		nil, // Parent Dependencies Key
		nil, // Parent
		repeatJobKey,
	}

	if job.opts.Parent != nil {
		parentKey := job.opts.Parent.Queue + ":" + job.opts.Parent.ID
		args[4] = parentKey
		args[6] = parentKey + ":dependencies"
		parent := map[string]any{
			"id":       job.opts.Parent.ID,
			"queueKey": job.opts.Parent.Queue,
		}
		if job.opts.FailParentOnFailure {
			parent["fpof"] = true
		}
		if job.opts.RemoveDependencyOnFailure {
			parent["rdof"] = true
		}
		args[7] = parent
	}

	if job.opts.WaitChildren {
		args[5] = keyPrefix + "waiting-children"
	}

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal args: %w", err)
	}

	// repeatJobKey travels in the args (rjk hash field), not in the stored
	// opts; upstream's Job constructor destructures it out of opts.
	optsForWire := job.opts
	optsForWire.RepeatJobKey = ""
	msgPackedOpts, err := msgpack.Marshal(optsForWire)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal opts: %w", err)
	}

	return keys, []any{msgPackedArgs, job.data, msgPackedOpts}, nil
}

// parseAddJobResult converts the addJob script result into a job id,
// mapping negative codes to errors like upstream finishedErrors.
func parseAddJobResult(result any) (string, error) {
	switch v := result.(type) {
	case string:
		return v, nil
	case int64:
		if v < 0 {
			return "", finishedErrors(v, "", "addJob")
		}
		return strconv.FormatInt(v, 10), nil
	default:
		return "", fmt.Errorf("lua AddJob script returned unexpected type: %T", result)
	}
}

// addJobInternal adds a job to the queue without requiring a Queue instance.
func addJobInternal(ctx context.Context, client redis.Cmdable, keyPrefix string, job rawJob, jobID string) (string, error) {
	keys, argv, err := buildAddJobArgs(keyPrefix, job, jobID)
	if err != nil {
		return "", err
	}

	givenJobID, err := lua.AddJob(ctx, client, keys, argv...)
	if err != nil {
		return "", fmt.Errorf("failed to add job via Lua: %w", err)
	}

	return parseAddJobResult(givenJobID)
}

// toRepeatKeyOpts converts JobRepeatOptions to repeat.RepeatKeyOpts.
func toRepeatKeyOpts(opts *JobRepeatOptions) repeat.RepeatKeyOpts {
	return repeat.RepeatKeyOpts{
		EndDate: opts.EndDate,
		TZ:      opts.TZ,
		Pattern: opts.Pattern,
		Every:   opts.Every,
		JobId:   opts.JobID,
	}
}

// getNextMillis mirrors upstream getNextMillis (repeat.ts).
func getNextMillis(millis int64, opts *JobRepeatOptions) (int64, error) {
	if opts.Pattern != "" && opts.Every > 0 {
		return 0, fmt.Errorf("both .pattern and .every options are defined for this repeatable job")
	}

	if opts.Every > 0 {
		every := int64(opts.Every)
		next := (millis / every) * every
		if !opts.Immediately {
			next += every
		}
		return next, nil
	}

	currentDate := time.UnixMilli(millis)
	if opts.StartDate != 0 && opts.StartDate > millis {
		currentDate = time.UnixMilli(opts.StartDate)
	}

	// cron-parser matches fields in the process-local timezone unless utc/tz
	// is given, and throws on an invalid tz.
	loc := time.Local
	if opts.UTC {
		loc = time.UTC
	}
	if opts.TZ != "" {
		l, err := time.LoadLocation(opts.TZ)
		if err != nil {
			return 0, fmt.Errorf("invalid repeat tz '%s': %w", opts.TZ, err)
		}
		loc = l
	}

	parser := cr.NewParser(cr.SecondOptional | cr.Minute | cr.Hour | cr.Dom | cr.Month | cr.Dow)
	sched, err := parser.Parse(opts.Pattern)
	if err != nil {
		return 0, fmt.Errorf("invalid cron pattern '%s': %w", opts.Pattern, err)
	}
	next := sched.Next(currentDate.In(loc))
	if next.IsZero() {
		return 0, nil
	}
	// cron-parser enforces endDate inside the iterator: an occurrence past
	// endDate ends the chain (upstream swallows the throw, returns undefined).
	if opts.EndDate != 0 && next.UnixMilli() > opts.EndDate {
		return 0, nil
	}
	return next.UnixMilli(), nil
}

// addNextRepeatableJob mirrors upstream Repeat.addNextRepeatableJob: it computes
// the next iteration slot, verifies the repeatable has not been removed (unless
// skipCheckExists), registers the slot in the repeat zset and adds the delayed
// job instance. Returns nil without error when no further iteration is due.
func addNextRepeatableJobInternal(ctx context.Context, client redis.Cmdable, keyPrefix string, name string, jsonData string, opts JobOptions, skipCheckExists bool) (*rawJob, error) {
	if opts.Repeat == nil {
		return nil, fmt.Errorf("addNextRepeatableJob called without repeat options")
	}
	repeatOpts := *opts.Repeat

	prevMillis := opts.PrevMillis
	currentCount := 1
	if repeatOpts.Count != 0 {
		currentCount = repeatOpts.Count + 1
	}

	if repeatOpts.Limit > 0 && currentCount > repeatOpts.Limit {
		return nil, nil
	}

	now := time.Now().UnixMilli()
	if repeatOpts.EndDate != 0 && now > repeatOpts.EndDate {
		return nil, nil
	}
	if prevMillis > now {
		now = prevMillis
	}

	nextMillis, err := getNextMillis(now, &repeatOpts)
	if err != nil {
		return nil, err
	}
	if nextMillis == 0 {
		return nil, nil
	}

	// The stored offset persists across iterations (upstream's
	// { offset, ...filteredRepeatOpts } spread keeps the previous value when
	// no fresh one is computed), keeping every+immediately chains at
	// slot+offset forever.
	hasImmediately := (repeatOpts.Every > 0 || repeatOpts.Pattern != "") && repeatOpts.Immediately
	offset := repeatOpts.Offset
	if hasImmediately {
		offset = now - nextMillis
	}

	// Store the undecorated custom jobId into the repeat options.
	if prevMillis == 0 && opts.JobID != "" {
		repeatOpts.JobID = opts.JobID
	}

	repeatJobKey := repeat.GetKey(name, toRepeatKeyOpts(&repeatOpts))

	// createNextJob
	jobID := repeat.GetJobId(name, strconv.FormatInt(nextMillis, 10), utils.MD5Hash(repeatJobKey), repeatOpts.JobID)
	nowMs := time.Now().UnixMilli()
	delay := nextMillis + offset - nowMs
	if delay < 0 || hasImmediately {
		delay = 0
	}

	mergedOpts := opts
	mergedOpts.JobID = jobID
	mergedOpts.Delay = int(delay)
	mergedOpts.Timestamp = nowMs
	mergedOpts.PrevMillis = nextMillis
	mergedOpts.RepeatJobKey = repeatJobKey
	nextRepeat := repeatOpts
	nextRepeat.Count = currentCount
	nextRepeat.Offset = offset
	nextRepeat.Immediately = false // upstream strips immediately from stored opts
	mergedOpts.Repeat = &nextRepeat

	raw, err := newJob(name, jsonData, mergedOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create repeatable job instance %s: %w", name, err)
	}
	keys, argv, err := buildAddJobArgs(keyPrefix, raw, jobID)
	if err != nil {
		return nil, err
	}
	if err := lua.AddJobScript.Load(ctx, client).Err(); err != nil {
		return nil, fmt.Errorf("failed to load addJob script: %w", err)
	}

	repeatSetKey := keyPrefix + "repeat"
	watchKeys := make([]string, 0, 2)
	if !skipCheckExists {
		watchKeys = append(watchKeys, repeatSetKey)
	}
	if mergedOpts.Parent != nil {
		watchKeys = append(watchKeys, mergedOpts.Parent.Queue+":"+mergedOpts.Parent.ID)
	}
	errRepeatRemoved := errors.New("repeatable job removed while scheduling")
	var addedJobID string
	runTransaction := func(tx redis.Cmdable, pipe redis.Pipeliner) error {
		if !skipCheckExists {
			if err := tx.ZScore(ctx, repeatSetKey, repeatJobKey).Err(); err != nil {
				if errors.Is(err, redis.Nil) {
					return errRepeatRemoved
				}
				return err
			}
		}
		if mergedOpts.Parent != nil {
			parentKey := mergedOpts.Parent.Queue + ":" + mergedOpts.Parent.ID
			exists, existsErr := tx.Exists(ctx, parentKey).Result()
			if existsErr != nil {
				return existsErr
			}
			if exists == 0 {
				return fmt.Errorf("bullmq: missing key for parent job %s while executing addJob", mergedOpts.Parent.ID)
			}
		}
		pipe.ZAdd(ctx, repeatSetKey, redis.Z{Score: float64(nextMillis), Member: repeatJobKey})
		cmd := lua.AddJobScript.Run(ctx, pipe, keys, argv...)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		value, err := cmd.Result()
		if err != nil {
			return err
		}
		addedJobID, err = parseAddJobResult(value)
		return err
	}

	if len(watchKeys) == 0 {
		if err := runTransaction(client, client.TxPipeline()); err != nil {
			return nil, fmt.Errorf("failed to add repeatable job instance %s: %w", name, err)
		}
	} else {
		watcher, ok := client.(interface {
			Watch(context.Context, func(*redis.Tx) error, ...string) error
		})
		if !ok {
			return nil, fmt.Errorf("redis client does not support WATCH required for atomic repeat scheduling")
		}
		var transactionErr error
		for attempt := 0; attempt < 16; attempt++ {
			transactionErr = watcher.Watch(ctx, func(tx *redis.Tx) error {
				return runTransaction(tx, tx.TxPipeline())
			}, watchKeys...)
			if !errors.Is(transactionErr, redis.TxFailedErr) {
				break
			}
		}
		if errors.Is(transactionErr, errRepeatRemoved) {
			return nil, nil
		}
		if transactionErr != nil {
			return nil, fmt.Errorf("failed to add repeatable job instance %s: %w", name, transactionErr)
		}
	}

	raw.id = addedJobID
	raw.setJobContext(client, keyPrefix)

	return &raw, nil
}

// DrainOptions configures the Drain operation.
type DrainOptions struct {
	IncludeDelayed bool
}

// Drain removes all jobs from the queue.
func (q *Queue[D]) Drain(ctx context.Context, opts *DrainOptions) error {
	keys := []string{
		q.toKey("wait"),
		q.toKey("paused"),
	}

	includeDelayed := false
	if opts != nil {
		includeDelayed = opts.IncludeDelayed
	}

	if includeDelayed {
		keys = append(keys, q.toKey("delayed"))
	} else {
		keys = append(keys, "")
	}
	keys = append(keys, q.toKey("prioritized"))

	// The drain script returns no value; go-redis reports that as redis.Nil.
	_, err := lua.Drain(ctx, q.client, keys, q.keyPrefix)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to drain queue: %w", err)
	}
	return nil
}

// Clean removes jobs from the queue based on the specified criteria.
func (q *Queue[D]) Clean(ctx context.Context, grace time.Duration, limit int, state JobState) ([]string, error) {
	var jobs []string

	timestamp := time.Now().UnixMilli() - grace.Milliseconds()

	// The waiting state lives under the "wait" key.
	set := state.redisKey()

	keys := []string{
		q.toKey(set),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(ctx, q.client, keys, q.keyPrefix, timestamp, limit, set)
	if err != nil {
		return jobs, fmt.Errorf("failed to clean jobs: %w", err)
	}

	if result, ok := i.([]string); ok {
		jobs = result
	} else if resultSlice, ok := i.([]any); ok {
		jobs = make([]string, 0, len(resultSlice))
		for _, v := range resultSlice {
			if s, ok := v.(string); ok {
				jobs = append(jobs, s)
			}
		}
	} else {
		return nil, fmt.Errorf("unexpected result type from clean: %T", i)
	}

	q.emit("cleaned", jobs, state)
	return jobs, nil
}

// ObliterateOptions configures the Obliterate operation.
type ObliterateOptions struct {
	Force bool
	Count int
}

// Obliterate completely removes the queue and its data.
func (q *Queue[D]) Obliterate(ctx context.Context, opts *ObliterateOptions) error {
	if err := q.pause(ctx, true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}

	force := ""
	count := 1000
	if opts != nil {
		if opts.Force {
			force = "force"
		}
		if opts.Count > 0 {
			count = opts.Count
		}
	}

	keys := []string{
		q.toKey("meta"),
		q.keyPrefix,
	}

	for {
		i, err := lua.Obliterate(ctx, q.client, keys, count, force)
		if err != nil {
			return fmt.Errorf("failed to obliterate queue: %w", err)
		}

		result, ok := i.(int64)
		if !ok {
			return fmt.Errorf("unexpected result type from obliterate: %T", i)
		}

		if result < 0 {
			switch result {
			case -1:
				return ErrQueueNotPaused
			case -2:
				return ErrQueueHasActive
			}
		} else if result == 0 {
			break
		}
	}

	return nil
}

// Ping checks the connection to the Redis server.
func (q *Queue[D]) Ping(ctx context.Context) error {
	_, err := q.client.Ping(ctx).Result()
	return err
}

// toKey constructs a Redis key with the queue's prefix.
func (q *Queue[D]) toKey(name string) string {
	return q.keyPrefix + name
}

// Remove removes a job from the queue by its ID. Child jobs are kept; use
// RemoveWithChildren to remove them too.
func (q *Queue[D]) Remove(ctx context.Context, jobID string) error {
	return jobRemove(ctx, q.client, q.keyPrefix, jobID, false)
}

// RemoveWithChildren removes a job and its children from the queue by its ID.
func (q *Queue[D]) RemoveWithChildren(ctx context.Context, jobID string) error {
	return jobRemove(ctx, q.client, q.keyPrefix, jobID, true)
}

// TrimEvents trims the event stream to approximately the maximum length
// (XTRIM MAXLEN ~, like upstream trimEvents).
func (q *Queue[D]) TrimEvents(ctx context.Context, max int64) (int64, error) {
	return q.client.XTrimMaxLenApprox(ctx, q.keyPrefix+"events", max, 0).Result()
}

// Close releases the queue's resources and prevents further operations.
// The Redis client is externally managed and is not closed.
func (q *Queue[D]) Close() error {
	q.closed.Store(true)
	q.ee.RemoveAll()
	return nil
}

// ensureMeta writes the queue meta fields JS clients expect (upstream does
// this in the Queue constructor); best-effort, once per Queue instance.
func (q *Queue[D]) ensureMeta(ctx context.Context) {
	q.metaOnce.Do(func() {
		_ = q.client.HSet(ctx, q.toKey("meta"), "opts.maxLenEvents", strconv.FormatInt(q.streamEventsMaxLen, 10)).Err()
	})
}

// InitStream writes the events stream maxlen metadata. Jobs added through
// this queue write it automatically; calling this is only needed when the
// queue instance is used exclusively for reads.
func (q *Queue[D]) InitStream(ctx context.Context) error {
	if err := q.client.HSet(ctx, q.toKey("meta"), "opts.maxLenEvents", strconv.FormatInt(q.streamEventsMaxLen, 10)).Err(); err != nil {
		return fmt.Errorf("failed to set meta opts.maxLenEvents: %w", err)
	}
	return nil
}
