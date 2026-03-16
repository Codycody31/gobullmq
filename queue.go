package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
}

// QueueOptions holds configuration options for creating a new Queue.
type QueueOptions struct {
	Prefix             string
	StreamEventsMaxLen int64
}

// BulkJob defines a job for AddBulk.
type BulkJob[D any] struct {
	Name string
	Data D
	Opts JobOptions
}

// QueueName returns the queue name.
func (q *Queue[D]) QueueName() string {
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
	}

	q := &Queue[D]{
		name:               name,
		token:              uuid.New(),
		client:             client,
		prefix:             prefix,
		keyPrefix:          prefix + ":" + name + ":",
		streamEventsMaxLen: streamEventsMaxLen,
	}

	q.ee = eventemitter.NewEventEmitter()

	return q, nil
}

// Emit emits the event with the given name and arguments.
func (q *Queue[D]) Emit(event string, args ...interface{}) {
	q.ee.Emit(event, args...)
}

// On listens for the event and returns a ListenerID that can be used with Off.
func (q *Queue[D]) On(event string, listener func(...interface{})) eventemitter.ListenerID {
	return q.ee.On(event, listener)
}

// Off removes a specific listener by its ListenerID.
func (q *Queue[D]) Off(event string, id eventemitter.ListenerID) {
	q.ee.RemoveListener(event, id)
}

// Once listens for the event only once and returns a ListenerID.
func (q *Queue[D]) Once(event string, listener func(...interface{})) eventemitter.ListenerID {
	return q.ee.Once(event, listener)
}

// Add adds a new job to the queue.
func (q *Queue[D]) Add(ctx context.Context, jobName string, data D, addOpts ...AddOption) (*Job[D], error) {
	if q.closed.Load() {
		return nil, ErrQueueClosed
	}

	opts := &JobOptions{
		Attempts:  1,
		Timestamp: time.Now().UnixMilli(),
	}

	for _, fn := range addOpts {
		fn(opts)
	}

	if opts.JobID != "" {
		if opts.JobID == "0" || (len(opts.JobID) > 1 && opts.JobID[0] == '0' && opts.JobID[1] != ':') {
			return nil, fmt.Errorf("jobId cannot be '0' or start with '0:' unless it's a delayed job marker")
		}
	}

	if jobName == "" {
		jobName = _DEFAULT_JOB_NAME
	}

	jsonDataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal jobData to JSON: %w", err)
	}
	jsonData := string(jsonDataBytes)

	if opts.Repeat != nil && (opts.Repeat.Every != 0 || opts.Repeat.Pattern != "") {
		return q.addRepeatableJob(ctx, jobName, data, jsonData, *opts, true)
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

	q.Emit("waiting", raw)

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

	_, err := lua.Pause(ctx, q.client, keys, p)
	if err != nil {
		return fmt.Errorf("failed to pause or resume queue: %w", err)
	}

	return nil
}

// Pause pauses the queue, preventing new jobs from being processed.
func (q *Queue[D]) Pause(ctx context.Context) error {
	if err := q.pause(ctx, true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}
	q.Emit("paused")
	return nil
}

// Resume resumes the queue, allowing jobs to be processed.
func (q *Queue[D]) Resume(ctx context.Context) error {
	if err := q.pause(ctx, false); err != nil {
		return fmt.Errorf("failed to resume queue: %w", err)
	}
	q.Emit("resumed")
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

// addJobInternal adds a job to the queue without requiring a Queue instance.
func addJobInternal(ctx context.Context, client redis.Cmdable, keyPrefix string, job rawJob, jobID string) (string, error) {
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

	args := []interface{}{
		keyPrefix,
		jobID,
		job.name,
		job.timestamp,
		nil, // Parent Key
		nil, // Wait Children Key
		nil, // Parent Dependencies Key
		nil, // Parent ID
		job.opts.RepeatJobKey,
	}

	if job.opts.Parent != nil {
		parentKey := job.opts.Parent.Queue + ":" + job.opts.Parent.ID
		args[4] = parentKey
		args[6] = parentKey + ":dependencies"
		args[7] = job.opts.Parent.ID
	}

	if job.opts.WaitChildren {
		args[5] = keyPrefix + "waiting-children"
	}

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("failed to marshal args: %w", err)
	}

	msgPackedOpts, err := msgpack.Marshal(job.opts)
	if err != nil {
		return "", fmt.Errorf("failed to marshal opts: %w", err)
	}

	givenJobID, err := lua.AddJob(ctx, client, keys, msgPackedArgs, job.data, msgPackedOpts)
	if err != nil {
		return "", fmt.Errorf("failed to add job via Lua: %w", err)
	}

	finalJobID, ok := givenJobID.(string)
	if !ok {
		return "", fmt.Errorf("lua AddJob script returned unexpected type: %T", givenJobID)
	}

	return finalJobID, nil
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

// scheduleNextRepeatableJob calculates and schedules the next instance of a repeatable job.
func (q *Queue[D]) scheduleNextRepeatableJob(ctx context.Context, name string, jsonData string, opts JobOptions) error {
	return scheduleNextRepeatableJobInternal(ctx, q.client, q.keyPrefix, name, jsonData, opts)
}

// scheduleNextRepeatableJobInternal is a standalone function that schedules the next repeatable job
// without requiring a Queue instance. This avoids creating throwaway Queue objects.
func scheduleNextRepeatableJobInternal(ctx context.Context, client redis.Cmdable, keyPrefix string, name string, jsonData string, opts JobOptions) error {
	if opts.Repeat == nil {
		return fmt.Errorf("scheduleNextRepeatableJob called without repeat options")
	}

	baseMillis := int64(opts.Repeat.PrevMillis)
	if baseMillis == 0 {
		baseMillis = opts.Timestamp
	}

	nextMillis, err := calculateNextMillis(baseMillis, opts.Repeat)
	if err != nil {
		return fmt.Errorf("failed to calculate next repeat time: %w", err)
	}

	if nextMillis == 0 {
		return nil
	}

	repeatJobKey := repeat.GetKey(name, toRepeatKeyOpts(opts.Repeat))
	jobID, err := repeat.GetJobId(name, nextMillis, utils.MD5Hash(repeatJobKey), "")
	if err != nil {
		return fmt.Errorf("failed to get next repeatable job id for %s: %w", name, err)
	}

	currentUnixMillis := time.Now().UnixMilli()
	delay := nextMillis - currentUnixMillis
	if delay < 0 {
		delay = 0
	}

	nextOpts := opts
	nextOpts.JobID = jobID
	nextOpts.Delay = int(delay)
	nextOpts.Timestamp = currentUnixMillis
	nextOpts.Repeat.PrevMillis = int(nextMillis)
	nextOpts.RepeatJobKey = repeatJobKey
	nextOpts.Repeat.Count = opts.Repeat.Count + 1

	_, err = client.ZAdd(ctx, keyPrefix+"repeat", redis.Z{
		Score:  float64(nextMillis),
		Member: repeatJobKey,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to update repeat set for key %s: %w", repeatJobKey, err)
	}

	raw, err := newJob(name, jsonData, nextOpts)
	if err != nil {
		return fmt.Errorf("failed to create next repeatable job instance %s: %w", name, err)
	}
	addedJobID, err := addJobInternal(ctx, client, keyPrefix, raw, jobID)
	if err != nil {
		return fmt.Errorf("failed to add next repeatable job instance %s: %w", name, err)
	}
	if addedJobID != jobID {
		raw.id = addedJobID
	} else {
		raw.id = jobID
	}

	return nil
}

// calculateNextMillis calculates the next execution time in milliseconds.
func calculateNextMillis(lastExecMillis int64, opts *JobRepeatOptions) (int64, error) {
	if opts == nil {
		return 0, nil
	}

	if opts.Limit > 0 && opts.Count >= opts.Limit {
		return 0, nil
	}
	nowMillis := time.Now().UnixMilli()
	if opts.EndDate != nil && nowMillis >= opts.EndDate.UnixMilli() {
		return 0, nil
	}

	var next time.Time
	baseTime := time.UnixMilli(lastExecMillis)

	if opts.Pattern != "" {
		loc := time.UTC
		if opts.TZ != "" {
			locAttempt, err := time.LoadLocation(opts.TZ)
			if err == nil {
				loc = locAttempt
			}
		}
		parser := cr.NewParser(cr.Second | cr.Minute | cr.Hour | cr.Dom | cr.Month | cr.Dow)
		sched, err := parser.Parse(opts.Pattern)
		if err != nil {
			return 0, fmt.Errorf("invalid cron pattern '%s': %w", opts.Pattern, err)
		}
		next = sched.Next(baseTime.In(loc))
	} else if opts.Every > 0 {
		duration := time.Duration(opts.Every) * time.Millisecond
		next = baseTime.Add(duration)
	} else {
		return 0, nil
	}

	if opts.StartDate != nil && next.Before(*opts.StartDate) {
		if opts.Every > 0 {
			next = *opts.StartDate
		} else {
			baseTime = opts.StartDate.Add(-1 * time.Millisecond)
			loc := time.UTC
			if opts.TZ != "" {
				locAttempt, err := time.LoadLocation(opts.TZ)
				if err == nil {
					loc = locAttempt
				}
			}
			parser := cr.NewParser(cr.Second | cr.Minute | cr.Hour | cr.Dom | cr.Month | cr.Dow)
			sched, err := parser.Parse(opts.Pattern)
			if err != nil {
				return 0, fmt.Errorf("invalid cron pattern '%s': %w", opts.Pattern, err)
			}
			next = sched.Next(baseTime.In(loc))
		}
	}

	if opts.EndDate != nil && next.After(*opts.EndDate) {
		return 0, nil
	}

	if next.IsZero() {
		return 0, nil
	}

	return next.UnixMilli(), nil
}

// addRepeatableJob is called during the initial Queue.Add call.
func (q *Queue[D]) addRepeatableJob(ctx context.Context, name string, data D, jsonData string, opts JobOptions, skipCheckExists bool) (*Job[D], error) {
	if opts.Repeat == nil {
		return nil, fmt.Errorf("addRepeatableJob called without repeat options")
	}

	initialBaseMillis := time.Now().UnixMilli()
	if opts.Repeat.StartDate != nil && initialBaseMillis < opts.Repeat.StartDate.UnixMilli() {
		initialBaseMillis = opts.Repeat.StartDate.UnixMilli()
	}

	nextMillis, err := calculateNextMillis(initialBaseMillis, opts.Repeat)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate initial nextMillis: %w", err)
	}
	if nextMillis == 0 {
		return nil, fmt.Errorf("repeatable job will not run based on options")
	}

	repeatJobKey := repeat.GetKey(name, toRepeatKeyOpts(opts.Repeat))

	repeatableExists := true
	if !skipCheckExists {
		score, err := q.client.ZScore(ctx, q.toKey("repeat"), repeatJobKey).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to check repeatable job existence: %w", err)
		}
		if err == redis.Nil || score == 0 {
			repeatableExists = false
		}
	}

	if skipCheckExists || !repeatableExists {
		jobID, err := repeat.GetJobId(name, nextMillis, utils.MD5Hash(repeatJobKey), "")
		if err != nil {
			return nil, fmt.Errorf("failed to get initial repeatable job id: %w", err)
		}

		currentUnixMillis := time.Now().UnixMilli()
		delay := nextMillis - currentUnixMillis
		if delay < 0 {
			delay = 0
		}

		firstOpts := opts
		firstOpts.JobID = jobID
		firstOpts.Delay = int(delay)
		firstOpts.Timestamp = currentUnixMillis
		firstOpts.Repeat.PrevMillis = int(nextMillis)
		firstOpts.RepeatJobKey = repeatJobKey
		firstOpts.Repeat.Count = 1

		_, err = q.client.ZAdd(ctx, q.toKey("repeat"), redis.Z{
			Score:  float64(nextMillis),
			Member: repeatJobKey,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to add repeat set entry for key %s: %w", repeatJobKey, err)
		}

		raw, err := newJob(name, jsonData, firstOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial repeatable job instance: %w", err)
		}
		addedJobID, err := q.addJob(ctx, raw, jobID)
		if err != nil {
			return nil, fmt.Errorf("failed to add initial repeatable job instance: %w", err)
		}
		if addedJobID != jobID {
			raw.id = addedJobID
		} else {
			raw.id = jobID
		}

		q.Emit("waiting", raw)

		return &Job[D]{raw: &raw, data: data}, nil
	}

	return nil, fmt.Errorf("repeatable job with key %s already exists", repeatJobKey)
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

	_, err := lua.Drain(ctx, q.client, keys, q.keyPrefix)
	if err != nil {
		return fmt.Errorf("failed to drain queue: %w", err)
	}
	return nil
}

// Clean removes jobs from the queue based on the specified criteria.
func (q *Queue[D]) Clean(ctx context.Context, grace time.Duration, limit int, state JobState) ([]string, error) {
	var jobs []string

	timestamp := time.Now().Unix() - int64(grace.Seconds())

	keys := []string{
		q.toKey(string(state)),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(ctx, q.client, keys, q.keyPrefix, timestamp, limit, string(state))
	if err != nil {
		return jobs, fmt.Errorf("failed to clean jobs: %w", err)
	}

	if result, ok := i.([]string); ok {
		jobs = result
	} else if resultSlice, ok := i.([]interface{}); ok {
		jobs = make([]string, 0, len(resultSlice))
		for _, v := range resultSlice {
			if s, ok := v.(string); ok {
				jobs = append(jobs, s)
			}
		}
	} else {
		return nil, fmt.Errorf("unexpected result type from clean: %T", i)
	}

	q.Emit("cleaned", jobs, string(state))
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

// Remove removes a job from the queue by its ID.
func (q *Queue[D]) Remove(ctx context.Context, jobID string, removeChildren bool) error {
	keys := []string{
		q.keyPrefix,
	}

	i, err := lua.RemoveJob(ctx, q.client, keys, jobID, removeChildren)
	if err != nil {
		return fmt.Errorf("failed to remove job: %w", err)
	}

	code, ok := i.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from remove: %T", i)
	}
	if code == 0 {
		return ErrJobLocked
	}

	return nil
}

// TrimEvents trims the event stream to the specified maximum length.
func (q *Queue[D]) TrimEvents(ctx context.Context, max int64) (int64, error) {
	return q.client.XTrimMaxLen(ctx, q.keyPrefix+"events", max).Result()
}

// Close releases the queue's resources and prevents further operations.
func (q *Queue[D]) Close(ctx context.Context) error {
	q.closed.Store(true)
	q.ee.RemoveAll()
	return nil
}

// InitStream initializes the events stream with maxlen metadata.
func (q *Queue[D]) InitStream(ctx context.Context) error {
	if _, err := q.client.XTrimMaxLen(ctx, q.toKey("events"), q.streamEventsMaxLen).Result(); err != nil {
		return fmt.Errorf("failed to set initial trim for events stream: %w", err)
	}
	if _, err := q.client.HSet(ctx, q.toKey("meta"), "opts.maxLenEvents", strconv.FormatInt(q.streamEventsMaxLen, 10)).Result(); err != nil {
		return fmt.Errorf("failed to set meta opts.maxLenEvents: %w", err)
	}
	return nil
}
