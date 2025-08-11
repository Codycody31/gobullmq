package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	cr "github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
	"go.codycody31.dev/gobullmq/types"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/redisAction"
)

// QueueIface defines the interface for a job queue with various operations.
type QueueIface interface {
	// EventEmitterIface is embedded to provide event handling capabilities.
	eventemitter.EventEmitterIface
	// Init initializes a new queue with the given context, name, and options.
	Init(ctx context.Context, name string, opts QueueOptions) (*Queue, error)
	// Add adds a new job to the queue with the specified name, data, and options.
	Add(ctx context.Context, jobName string, jobData interface{}, addOpts ...AddOption) (types.Job, error)
	// Pause pauses the queue, preventing new jobs from being processed.
	Pause(ctx context.Context) error
	// Resume resumes the queue, allowing jobs to be processed.
	Resume(ctx context.Context) error
	// IsPaused checks if the queue is currently paused.
	IsPaused(ctx context.Context) (bool, error)
	// Drain removes all jobs from the queue, optionally including delayed jobs.
	Drain(delayed bool) error
	// Clean removes completed jobs from the queue based on the specified criteria.
	Clean(grace int, limit int, cType types.QueueEventType) ([]string, error)
	// Obliterate completely removes the queue and its data.
	Obliterate(opts ObliterateOpts) error
	// Ping checks the connection to the Redis server.
	Ping() error
	// Remove removes a job from the queue by its ID.
	Remove(jobId string, removeChildren bool) error
	// TrimEvents trims the event stream to the specified maximum length.
	TrimEvents(max int64) (int64, error)
}

var _ QueueIface = (*Queue)(nil)

// Queue represents a job queue with various operations.
type Queue struct {
	eventemitter.EventEmitter
	Name      string
	Token     uuid.UUID
	KeyPrefix string
	Client    redis.Cmdable
	Prefix    string
	ctx       context.Context
}

// QueueOption holds configuration options for creating a new Queue.
// Renamed from the original QueueOption to avoid conflict with the functional option type.
type QueueOptions struct {
	// RedisOpts specifies the connection options for the Redis client.
	// It's recommended to provide this instead of separate IP/Passwd/Mode.
	RedisOpts *redis.Options

	// DEPRECATED: Use RedisOpts instead.
	Mode int
	// DEPRECATED: Use RedisOpts instead.
	RedisIp string
	// DEPRECATED: Use RedisOpts instead.
	RedisPasswd string

	// KeyPrefix is the prefix used for all Redis keys associated with this queue.
	// Defaults to "bull" if empty.
	KeyPrefix string

	// StreamsEventsMaxLen is the maximum approximate length for the events stream.
	// Defaults to 10000 if not set.
	StreamsEventsMaxLen int64
}

// QueueFunctionalOption defines the type for functional options used with NewQueue.
type QueueFunctionalOption func(*QueueOptions)

type QueueJob struct {
	Name string
	Data types.JobData
	Opts types.JobOptions
}

// NewQueue creates a new Queue instance with the specified context, name, and options.
// It uses the functional options pattern for configuration.
func NewQueue(ctx context.Context, name string, functionalOpts ...QueueFunctionalOption) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("queue name must be provided")
	}

	// Default options
	opts := QueueOptions{
		KeyPrefix:           "bull",
		StreamsEventsMaxLen: 10000, // Default maxlen
	}

	// Apply functional options
	for _, fn := range functionalOpts {
		fn(&opts)
	}

	q := &Queue{
		Name:  name,
		Token: uuid.New(),
		ctx:   ctx,
	}

	q.EventEmitter.Init()

	q.KeyPrefix = opts.KeyPrefix // Use the resolved prefix
	q.Prefix = q.KeyPrefix
	q.KeyPrefix = q.KeyPrefix + ":" + name + ":"

	// --- Redis Client Initialization ---
	var redisClient redis.Cmdable
	var err error

	if opts.RedisOpts != nil {
		// Preferred method: Use provided redis.Options
		// TODO: Add support for different Redis client types (Cluster, Sentinel) if needed
		redisClient = redis.NewClient(opts.RedisOpts)
	} else if opts.RedisIp != "" {
		// Legacy/Deprecated method: Use individual fields
		// This uses the internal redisAction.Init, which might need refactoring later
		// to directly return a Cmdable based on redis.Options.
		redisClient, err = redisAction.Init(opts.RedisIp, opts.RedisPasswd, opts.Mode)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Redis client using legacy options: %w", err)
		}
	} else {
		// No Redis options provided, try connecting to default localhost
		// Or return an error, depending on desired behavior.
		// For now, let's assume default localhost might work.
		redisClient = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		// Or return an error:
		// return nil, fmt.Errorf("Redis connection options must be provided either via RedisOpts or legacy fields")
	}

	q.Client = redisClient
	// --- End Redis Client Initialization ---

	// Check connection
	if err := q.Ping(); err != nil {
		// Consider closing the client if ping fails?
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Set stream max length
	if _, err := q.Client.XTrimMaxLen(q.ctx, q.toKey("events"), opts.StreamsEventsMaxLen).Result(); err != nil {
		// Log error but don't fail queue creation? Or should this be fatal?
		q.Emit("error", fmt.Sprintf("Failed to set initial trim for events stream: %v", err))
	}
	// Set maxlen in meta (redundant if XTRIM works, but matches JS behaviour)
	if _, err := q.Client.HSet(q.ctx, q.toKey("meta"), "opts.maxLenEvents", strconv.FormatInt(opts.StreamsEventsMaxLen, 10)).Result(); err != nil {
		q.Emit("error", fmt.Sprintf("Failed to set meta opts.maxLenEvents: %v", err))
	}

	return q, nil
}

// --- Functional Options for NewQueue ---

// WithRedisOptions specifies the Redis connection options.
func WithRedisOptions(redisOpts *redis.Options) QueueFunctionalOption {
	return func(opts *QueueOptions) {
		opts.RedisOpts = redisOpts
	}
}

// WithKeyPrefix sets the key prefix for Redis keys.
func WithKeyPrefix(prefix string) QueueFunctionalOption {
	return func(opts *QueueOptions) {
		if prefix != "" {
			opts.KeyPrefix = prefix
		}
	}
}

// WithStreamsEventsMaxLen sets the max length for the events stream.
func WithStreamsEventsMaxLen(maxLen int64) QueueFunctionalOption {
	return func(opts *QueueOptions) {
		if maxLen > 0 {
			opts.StreamsEventsMaxLen = maxLen
		}
	}
}

// DEPRECATED: Use WithRedisOptions instead.
func WithLegacyRedisConfig(ip string, password string, mode int) QueueFunctionalOption {
	return func(opts *QueueOptions) {
		opts.RedisIp = ip
		opts.RedisPasswd = password
		opts.Mode = mode
	}
}

// --- End Functional Options ---

// Init initializes a new Queue instance with the specified context, name, and options.
// DEPRECATED: Use NewQueue directly with functional options.
func (q *Queue) Init(ctx context.Context, name string, legacyOpts QueueOptions) (*Queue, error) {
	// Convert legacy options to functional options for compatibility
	functionalOpts := []QueueFunctionalOption{
		WithKeyPrefix(legacyOpts.KeyPrefix),
		WithStreamsEventsMaxLen(legacyOpts.StreamsEventsMaxLen),
	}
	if legacyOpts.RedisOpts != nil {
		functionalOpts = append(functionalOpts, WithRedisOptions(legacyOpts.RedisOpts))
	} else if legacyOpts.RedisIp != "" {
		functionalOpts = append(functionalOpts, WithLegacyRedisConfig(legacyOpts.RedisIp, legacyOpts.RedisPasswd, legacyOpts.Mode))
	}

	return NewQueue(ctx, name, functionalOpts...)
}

// Add adds a new job to the queue with the specified name, data, and functional options.
// jobData can be any value that is marshallable to JSON.
func (q *Queue) Add(ctx context.Context, jobName string, jobData interface{}, addOpts ...AddOption) (types.Job, error) {
	// Default options
	opts := &types.JobOptions{
		Attempts:  1, // Default attempts
		TimeStamp: time.Now().UnixMilli(),
		// Default RemoveOnComplete/Fail will be handled by Lua scripts if nil
	}

	// Apply functional options
	for _, fn := range addOpts {
		fn(opts)
	}

	// Validate JobId if provided
	if opts.JobId != "" {
		if opts.JobId == "0" || (len(opts.JobId) > 1 && opts.JobId[0] == '0' && opts.JobId[1] != ':') {
			return types.Job{}, fmt.Errorf("jobId cannot be '0' or start with '0:' unless it's a delayed job marker")
		}
	}

	// Default job name
	if jobName == "" {
		jobName = _DEFAULT_JOB_NAME
	}

	// Marshal job data
	jsonDataBytes, err := json.Marshal(jobData)
	if err != nil {
		return types.Job{}, fmt.Errorf("failed to marshal jobData to JSON: %w", err)
	}
	jsonData := string(jsonDataBytes)

	// Handle repeatable jobs
	if opts.Repeat != nil && (opts.Repeat.Every != 0 || opts.Repeat.Pattern != "") {
		// TODO: Revisit addRepeatableJob logic to align with marshaled data and context passing
		// For now, let's pass the marshaled data and context.
		return q.addRepeatableJob(ctx, jobName, jsonData, *opts, true)
	}

	// Create and add standard job
	job, err := newJob(jobName, jsonData, *opts) // Pass marshaled data
	if err != nil {
		return job, fmt.Errorf("failed to create new job: %w", err)
	}
	jobId, err := q.addJob(ctx, job, opts.JobId) // Pass context
	if err != nil {
		return job, fmt.Errorf("failed to add job: %w", err)
	}
	job.Id = jobId

	q.Emit("waiting", job)

	return job, nil
}

// pause pauses or resumes the queue based on the provided flag.
func (q *Queue) pause(ctx context.Context, pause bool) error { // Added context
	client := q.Client
	p := "paused"

	src := "wait"
	dst := "paused"
	if !pause {
		src = "paused"
		dst = "wait"
		p = "resumed"
	}

	// Use the passed context
	exists, err := client.Exists(ctx, q.toKey(src)).Result()
	if err != nil {
		return fmt.Errorf("failed to check if queue exists: %w", err)
	}

	if exists == 0 {
		return fmt.Errorf("source queue does not exist, nothing to pause or resume")
	}

	keys := []string{
		q.toKey(src),
		q.toKey(dst),
		q.toKey("meta"),
		q.toKey("prioritized"),
		q.toKey("events"),
	}

	// Use the passed context
	_, err = lua.Pause(client, keys, p)
	if err != nil {
		return fmt.Errorf("failed to pause or resume queue: %w", err)
	}
	// fmt.Println("Result: ", rs) // Optional logging

	return nil
}

// Pause pauses the queue, preventing new jobs from being processed.
func (q *Queue) Pause(ctx context.Context) error { // Added context
	if err := q.pause(ctx, true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}
	q.Emit("paused")
	return nil
}

// Resume resumes the queue, allowing jobs to be processed.
func (q *Queue) Resume(ctx context.Context) error { // Added context
	if err := q.pause(ctx, false); err != nil {
		return fmt.Errorf("failed to resume queue: %w", err)
	}
	q.Emit("resumed")
	return nil
}

// IsPaused checks if the queue is currently paused.
func (q *Queue) IsPaused(ctx context.Context) (bool, error) { // Added context and error return
	pausedKeyExists, err := q.Client.HExists(ctx, q.KeyPrefix+"meta", "paused").Result()
	return pausedKeyExists, err
}

// addJob adds a job to the queue with the specified job ID.
func (q *Queue) addJob(ctx context.Context, job types.Job, jobId string) (string, error) { // Added context
	rdb := q.Client

	keys := []string{
		q.KeyPrefix + "wait",
		q.KeyPrefix + "paused",
		q.KeyPrefix + "meta",
		q.KeyPrefix + "id",
		q.KeyPrefix + "delayed",
		q.KeyPrefix + "prioritized",
		q.KeyPrefix + "completed",
		q.KeyPrefix + "events",
		q.KeyPrefix + "pc",
	}

	// Prepare Base arguments
	args := []interface{}{
		q.KeyPrefix,
		jobId, // Can be empty, Lua handles assigning ID
		job.Name,
		job.TimeStamp,
		nil, // Parent Key: Set below if Parent options exist
		nil, // Wait Children Key: Not directly used in AddJob, managed by parent/child logic
		nil, // Parent Dependencies Key: Not directly used in AddJob
		nil, // Parent ID: Set below if Parent options exist
		job.Opts.RepeatJobKey,
	}

	// Add parent info if available
	if job.Opts.Parent != nil {
		parentKey := job.Opts.Parent.Queue + ":" + job.Opts.Parent.Id // Construct parent job key
		args[4] = parentKey                                           // parentKey
		args[7] = job.Opts.Parent.Id                                  // parentId
		// Note: waitChildrenKey and parentDependenciesKey are derived in Lua or other scripts
	}

	msgPackedArgs, err := msgpack.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("failed to marshal args: %w", err)
	}

	// Marshal JobOptions using msgpack
	// Log the options being sent to Lua
	// fmt.Printf("--- Adding Job to Lua ---\n")
	// fmt.Printf("Job ID Arg: %s\n", jobId)
	// fmt.Printf("Job Name: %s\n", job.Name)
	// fmt.Printf("Data: %s\n", job.Data)
	// fmt.Printf("Opts: %+v\n", job.Opts)
	// ---------------
	msgPackedOpts, err := msgpack.Marshal(job.Opts)
	if err != nil {
		return "", fmt.Errorf("failed to marshal opts: %w", err)
	}

	// Pass context to Lua script execution
	// Use Tracef for detailed Lua call logging if needed
	// fmt.Printf("Calling lua.AddJob with keys: %v, argsPacked: %x, data: %s, optsPacked: %x\n", keys, msgPackedArgs, job.Data, msgPackedOpts)
	givenJobId, err := lua.AddJob(rdb, keys, msgPackedArgs, job.Data, msgPackedOpts)
	if err != nil {
		return "", fmt.Errorf("failed to add job via Lua: %w", err)
	}

	// Ensure result is string
	finalJobId, ok := givenJobId.(string)
	if !ok {
		// This shouldn't happen based on Lua script, but handle defensively
		return "", fmt.Errorf("lua AddJob script returned unexpected type: %T", givenJobId)
	}

	return finalJobId, nil
}

// scheduleNextRepeatableJob calculates and schedules the next instance of a repeatable job.
// It's intended to be called after a repeatable job instance completes successfully.
func (q *Queue) scheduleNextRepeatableJob(ctx context.Context, name string, jsonData string, opts types.JobOptions) error {
	if opts.Repeat == nil {
		return fmt.Errorf("scheduleNextRepeatableJob called without repeat options")
	}

	// Use the *previous scheduled execution time* as the base for the next calculation,
	// otherwise use the completed job's timestamp (opts.TimeStamp might be outdated if job was delayed significantly).
	baseMillis := int64(opts.Repeat.PrevMillis)
	if baseMillis == 0 {
		// Fallback if PrevMillis isn't set (shouldn't happen after first run)
		baseMillis = opts.TimeStamp
		// fmt.Printf("Warning: Repeat PrevMillis was 0 for job %s, using completion timestamp %d as base\n", opts.JobId, baseMillis)
	}

	// Calculate next execution time using the new function
	nextMillis, err := calculateNextMillis(baseMillis, opts.Repeat) // Pass correct base time
	if err != nil {
		// Error already includes job name context from calculateNextMillis if pattern invalid
		return fmt.Errorf("failed to calculate next repeat time: %w", err)
	}

	// Check limits and end date - calculateNextMillis handles this
	if nextMillis == 0 {
		// Limit reached or end date passed, do not schedule next job
		// fmt.Printf("Repeatable job %s finished repeating.\n", opts.JobId)
		// TODO: Optionally remove from repeat set?
		// repeatJobKey := repeat.GetKey(name, *opts.Repeat)
		// q.Client.ZRem(ctx, q.toKey("repeat"), repeatJobKey)
		return nil // Not an error, just finished repeating
	}

	// If nextMillis is valid, schedule the job
	repeatJobKey := repeat.GetKey(name, *opts.Repeat)
	jobId, err := repeat.GetJobId(name, nextMillis, utils.MD5Hash(repeatJobKey), "") // Generate new ID for the *next* instance
	if err != nil {
		return fmt.Errorf("failed to get next repeatable job id for %s: %w", name, err)
	}

	// Calculate delay relative to current time
	currentUnixMillis := time.Now().UnixMilli()
	delay := nextMillis - currentUnixMillis
	if delay < 0 {
		delay = 0 // Don't schedule in the past
	}

	// Prepare options for the *next* job instance
	nextOpts := opts // Copy base options
	nextOpts.JobId = jobId
	nextOpts.Delay = int(delay)
	nextOpts.TimeStamp = currentUnixMillis       // Timestamp of when this scheduling happens
	nextOpts.Repeat.PrevMillis = int(nextMillis) // Store calculated next run time
	nextOpts.RepeatJobKey = repeatJobKey
	nextOpts.Repeat.Count = opts.Repeat.Count + 1 // Increment count for the next instance

	// ---- Logging ----
	// fmt.Printf("--- Scheduling Next Repeatable Job ---\n")
	// fmt.Printf("Previous Job ID: %s\n", opts.JobId)
	// fmt.Printf("Job Name: %s\n", name)
	// fmt.Printf("Repeat Key: %s\n", repeatJobKey)
	// fmt.Printf("Base Time (ms): %d\n", baseMillis)
	// fmt.Printf("Calculated nextMillis: %d (Time: %s)\n", nextMillis, time.UnixMilli(nextMillis))
	// fmt.Printf("Current Time (ms): %d\n", currentUnixMillis)
	// fmt.Printf("Calculated Delay (ms): %d\n", delay)
	// fmt.Printf("Options for next job instance: %+v\n", nextOpts)
	// fmt.Printf("Repeat Options for next job instance: %+v\n", nextOpts.Repeat)
	// ---------------

	// Update the repeat set in Redis
	_, err = q.Client.ZAdd(ctx, q.toKey("repeat"), redis.Z{
		Score:  float64(nextMillis),
		Member: repeatJobKey,
	}).Result()
	if err != nil {
		// Log or handle error adding to repeat set
		q.Emit("error", fmt.Sprintf("Failed to update repeat set for key %s: %v", repeatJobKey, err))
		// Continue trying to add the job instance?
	}

	// Create the job instance with the modified opts (containing delay, instance jobId etc.)
	job, err := newJob(name, jsonData, nextOpts) // Pass marshaled data and updated opts
	if err != nil {
		return fmt.Errorf("failed to create next repeatable job instance %s: %w", name, err)
	}
	addedJobId, err := q.addJob(ctx, job, jobId) // Pass generated jobId
	if err != nil {
		return fmt.Errorf("failed to add next repeatable job instance %s: %w", name, err)
	}
	if addedJobId != jobId {
		// fmt.Printf("Warning: Added next repeatable job ID mismatch. Expected %s, Got %s\n", jobId, addedJobId)
		job.Id = addedJobId // Use the ID returned by Redis
	} else {
		job.Id = jobId
	}

	// Emit event for the *scheduled* job
	q.Emit("waiting", job)

	return nil
}

// calculateNextMillis calculates the next execution time in milliseconds
// based on the repeat options and the last execution time.
// Returns 0 if no further execution is scheduled.
func calculateNextMillis(lastExecMillis int64, opts *types.JobRepeatOptions) (int64, error) {
	if opts == nil {
		return 0, nil // Not a repeat job
	}

	// --- Check Stop Conditions --- //
	// Limit
	if opts.Limit > 0 && opts.Count >= opts.Limit {
		return 0, nil // Limit reached
	}
	// EndDate
	nowMillis := time.Now().UnixMilli()
	if opts.EndDate != nil && nowMillis >= opts.EndDate.UnixMilli() {
		return 0, nil // End date passed
	}

	// --- Calculate Next Time --- //
	var next time.Time
	baseTime := time.UnixMilli(lastExecMillis)

	if opts.Pattern != "" {
		// Cron pattern logic
		loc := time.UTC // Default to UTC
		if opts.TZ != "" {
			locAttempt, err := time.LoadLocation(opts.TZ)
			if err == nil {
				loc = locAttempt
			} else {
				// Log warning: Invalid timezone, falling back to UTC
				// fmt.Printf("Warning: Invalid timezone '%s' in repeat options, using UTC\n", opts.TZ)
			}
		}
		// Use cron library with alias
		parser := cr.NewParser(cr.Second | cr.Minute | cr.Hour | cr.Dom | cr.Month | cr.Dow)
		sched, err := parser.Parse(opts.Pattern)
		if err != nil {
			return 0, fmt.Errorf("invalid cron pattern '%s': %w", opts.Pattern, err)
		}
		// Get next time *after* the baseTime in the specified location
		next = sched.Next(baseTime.In(loc))

	} else if opts.Every > 0 {
		// Every interval logic
		duration := time.Duration(opts.Every) * time.Millisecond
		next = baseTime.Add(duration)
	} else {
		return 0, nil // No valid repeat definition
	}

	// --- Check StartDate and EndDate again against the calculated next time --- //
	if opts.StartDate != nil && next.Before(*opts.StartDate) {
		// If next calculated time is before start date, recalculate based on start date
		// (This might be complex for cron, might need adjustment based on desired behavior)
		// For 'every', we can just return the start date?
		if opts.Every > 0 {
			next = *opts.StartDate
		} else {
			// Recalculating cron based on start date needs careful thought.
			// For now, let's assume the first run after StartDate is desired.
			baseTime = opts.StartDate.Add(-1 * time.Millisecond) // Start check just before StartDate
			loc := time.UTC
			if opts.TZ != "" {
				locAttempt, err := time.LoadLocation(opts.TZ)
				if err == nil {
					loc = locAttempt
				}
			}
			// Use alias here too
			parser := cr.NewParser(cr.Second | cr.Minute | cr.Hour | cr.Dom | cr.Month | cr.Dow)
			sched, _ := parser.Parse(opts.Pattern) // Ignore error, parsed above
			next = sched.Next(baseTime.In(loc))
		}
	}

	if opts.EndDate != nil && next.After(*opts.EndDate) {
		return 0, nil // Calculated next time is after end date
	}

	if next.IsZero() {
		return 0, nil // Calculation resulted in zero time
	}

	return next.UnixMilli(), nil
}

// addRepeatableJob is called during the initial Queue.Add call.
// It calculates and schedules the first instance and updates the repeat set.
func (q *Queue) addRepeatableJob(ctx context.Context, name string, jsonData string, opts types.JobOptions, skipCheckExists bool) (types.Job, error) {
	if opts.Repeat == nil {
		return types.Job{}, fmt.Errorf("addRepeatableJob called without repeat options")
	}

	// Use time.Now() or StartDate for the initial calculation base
	initialBaseMillis := time.Now().UnixMilli()
	if opts.Repeat.StartDate != nil && initialBaseMillis < opts.Repeat.StartDate.UnixMilli() {
		initialBaseMillis = opts.Repeat.StartDate.UnixMilli()
	}

	// Calculate the *first* execution time
	nextMillis, err := calculateNextMillis(initialBaseMillis, opts.Repeat)
	if err != nil {
		return types.Job{}, fmt.Errorf("failed to calculate initial nextMillis: %w", err)
	}
	if nextMillis == 0 {
		// Job doesn't repeat (e.g., limit 0 or end date passed)
		return types.Job{}, fmt.Errorf("repeatable job will not run based on options")
	}

	repeatJobKey := repeat.GetKey(name, *opts.Repeat)

	// Check existence ONLY if skipCheckExists is false (e.g., when updating repeat options, not implemented yet)
	repeatableExists := true
	if !skipCheckExists {
		score, err := q.Client.ZScore(ctx, q.toKey("repeat"), repeatJobKey).Result()
		if err != nil && err != redis.Nil {
			return types.Job{}, fmt.Errorf("failed to check repeatable job existence: %w", err)
		}
		if err == redis.Nil || score == 0 {
			repeatableExists = false
		}
		// If it exists and we weren't skipping the check, maybe return an error or update?
		// For initial add (skipCheckExists=true), we proceed regardless.
	}

	// If skipCheckExists is true (initial add) or if the job didn't exist previously
	if skipCheckExists || !repeatableExists {
		jobId, err := repeat.GetJobId(name, nextMillis, utils.MD5Hash(repeatJobKey), "") // Generate ID for the *first* instance
		if err != nil {
			return types.Job{}, fmt.Errorf("failed to get initial repeatable job id: %w", err)
		}

		currentUnixMillis := time.Now().UnixMilli()
		delay := nextMillis - currentUnixMillis
		if delay < 0 {
			delay = 0
		}

		// Prepare options for the *first* job instance
		firstOpts := opts // Copy base options
		firstOpts.JobId = jobId
		firstOpts.Delay = int(delay)
		firstOpts.TimeStamp = currentUnixMillis       // Timestamp of when this initial add happens
		firstOpts.Repeat.PrevMillis = int(nextMillis) // Store calculated next run time
		firstOpts.RepeatJobKey = repeatJobKey
		firstOpts.Repeat.Count = 1 // First instance

		// ---- Logging ----
		// fmt.Printf("--- Adding Repeatable Job (Initial) ---\n")
		// fmt.Printf("Job Name: %s\n", name)
		// fmt.Printf("Repeat Key: %s\n", repeatJobKey)
		// fmt.Printf("Base Time (ms): %d\n", initialBaseMillis)
		// fmt.Printf("Calculated nextMillis: %d (Time: %s)\n", nextMillis, time.UnixMilli(nextMillis))
		// fmt.Printf("Current Time (ms): %d\n", currentUnixMillis)
		// fmt.Printf("Calculated Delay (ms): %d\n", delay)
		// fmt.Printf("Options for first job instance: %+v\n", firstOpts) // Log the full options
		// fmt.Printf("Repeat Options for first job instance: %+v\n", firstOpts.Repeat)
		// ---------------

		// Update the repeat set in Redis with the calculated nextMillis
		_, err = q.Client.ZAdd(ctx, q.toKey("repeat"), redis.Z{
			Score:  float64(nextMillis),
			Member: repeatJobKey,
		}).Result()
		if err != nil {
			q.Emit("error", fmt.Sprintf("Failed to add initial repeat set ZADD for key %s: %v", repeatJobKey, err))
			// Continue trying to add the first job instance anyway?
		}

		// Create and add the first job instance
		job, err := newJob(name, jsonData, firstOpts)
		if err != nil {
			return job, fmt.Errorf("failed to create initial repeatable job instance: %w", err)
		}
		addedJobId, err := q.addJob(ctx, job, jobId) // Pass generated jobId
		if err != nil {
			return job, fmt.Errorf("failed to add initial repeatable job instance: %w", err)
		}
		if addedJobId != jobId {
			// fmt.Printf("Warning: Added repeatable job ID mismatch. Expected %s, Got %s\n", jobId, addedJobId)
			job.Id = addedJobId // Use the ID returned by Redis
		} else {
			job.Id = jobId
		}

		q.Emit("waiting", job) // Event for the first instance

		return job, nil
	} else {
		// This case implies !skipCheckExists && repeatableExists, meaning we are trying to add
		// a repeatable job that already exists in the repeat set. Handle as an error or update?
		// For now, return an error.
		return types.Job{}, fmt.Errorf("repeatable job with key %s already exists", repeatJobKey)
	}
}

// Drain removes all jobs from the queue, optionally including delayed jobs.
func (q *Queue) Drain(delayed bool) error {
	keys := []string{
		q.toKey("wait"),
		q.toKey("paused"),
	}

	if delayed {
		keys = append(keys, q.toKey("delayed"))
	} else {
		keys = append(keys, "")
	}
	keys = append(keys, q.toKey("prioritized"))

	_, err := lua.Drain(q.Client, keys, q.KeyPrefix)
	if err != nil {
		return fmt.Errorf("failed to drain queue: %w", err)
	}
	return nil
}

// Clean removes completed jobs from the queue based on the specified criteria.
func (q *Queue) Clean(grace int, limit int, cType types.QueueEventType) ([]string, error) {
	var jobs []string

	set := cType
	timestamp := time.Now().Unix() - int64(grace)

	keys := []string{
		q.toKey(string(set)),
		q.toKey("events"),
	}

	i, err := lua.CleanJobsInSet(q.Client, keys, q.KeyPrefix, timestamp, limit, string(set))
	if err != nil {
		return jobs, fmt.Errorf("failed to clean jobs: %w", err)
	}

	jobs = i.([]string)

	q.Emit("cleaned", jobs, string(set))
	return jobs, nil
}

type ObliterateOpts struct {
	Force bool // Use force = true to force obliteration even with active jobs in the queue (default: false)
	Count int  // Use count with the maximum number of deleted keys per iteration (default: 1000)
}

// Obliterate completely removes the queue and its data.
func (q *Queue) Obliterate(opts ObliterateOpts) error {
	if err := q.pause(context.Background(), true); err != nil {
		return fmt.Errorf("failed to pause queue: %w", err)
	}

	var force string
	if opts.Force {
		force = "force"
	}

	count := opts.Count
	if count == 0 {
		count = 1000
	}

	keys := []string{
		q.toKey("meta"),
		q.KeyPrefix,
	}

	for {
		i, err := lua.Obliterate(q.Client, keys, count, force)
		if err != nil {
			return fmt.Errorf("failed to obliterate queue: %w", err)
		}

		result := i.(int64)

		if result < 0 {
			switch result {
			case -1:
				return fmt.Errorf("cannot obliterate non-paused queue")
			case -2:
				return fmt.Errorf("cannot obliterate queue with active jobs")
			}
		} else if result == 0 {
			break
		}
	}

	return nil
}

// Ping checks the connection to the Redis server.
func (q *Queue) Ping() error {
	return redisAction.Ping(q.Client)
}

// toKey constructs a Redis key with the queue's prefix.
func (q *Queue) toKey(name string) string {
	return q.KeyPrefix + name
}

// Remove removes a job from the queue by its ID.
func (q *Queue) Remove(jobId string, removeChildren bool) error {
	keys := []string{
		q.KeyPrefix,
	}

	i, err := lua.RemoveJob(q.Client, keys, jobId, removeChildren)
	if err != nil {
		return fmt.Errorf("failed to remove job: %w", err)
	}

	if i.(int64) == 0 {
		return fmt.Errorf("failed to remove job: %s, the job is locked", jobId)
	}

	return nil
}

// TrimEvents trims the event stream to the specified maximum length.
func (q *Queue) TrimEvents(max int64) (int64, error) {
	return q.Client.XTrimMaxLen(q.ctx, q.KeyPrefix+"events", max).Result()
}
