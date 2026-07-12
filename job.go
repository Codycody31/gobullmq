package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/lua"
)

// JobState represents the state of a job in the queue.
type JobState string

// Job states as returned by State (the getState Lua script returns
// "waiting", not "wait"; "wait" is the Redis key name, not the state name).
const (
	JobStateCompleted       JobState = "completed"
	JobStateWaiting         JobState = "waiting"
	JobStateActive          JobState = "active"
	JobStatePaused          JobState = "paused"
	JobStatePrioritized     JobState = "prioritized"
	JobStateDelayed         JobState = "delayed"
	JobStateFailed          JobState = "failed"
	JobStateWaitingChildren JobState = "waiting-children"
)

// redisKey returns the Redis key name backing a state. The waiting state
// lives under the "wait" key; every other state key matches its state name.
func (s JobState) redisKey() string {
	if s == JobStateWaiting {
		return "wait"
	}
	return string(s)
}

// SortOrder selects the ordering of paginated getters.
type SortOrder int

const (
	// SortAsc returns entries oldest-first.
	SortAsc SortOrder = iota
	// SortDesc returns entries newest-first.
	SortDesc
)

// Placement selects where a job re-enters the wait list.
type Placement int

const (
	// FIFO places the job at the end of the wait list.
	FIFO Placement = iota
	// LIFO places the job at the front of the wait list.
	LIFO
)

// EventType represents a queue event type emitted via Redis streams.
type EventType string

// Event types written to the queue's Redis event stream.
const (
	EventCompleted        EventType = "completed"
	EventWaiting          EventType = "waiting"
	EventActive           EventType = "active"
	EventPaused           EventType = "paused"
	EventPrioritized      EventType = "prioritized"
	EventDelayed          EventType = "delayed"
	EventFailed           EventType = "failed"
	EventWaitingChildren  EventType = "waiting-children"
	EventRemoved          EventType = "removed"
	EventDuplicated       EventType = "duplicated"
	EventRetriesExhausted EventType = "retries-exhausted"
	EventDrained          EventType = "drained"
	EventProgress         EventType = "progress"
	EventStalled          EventType = "stalled"
	EventAdded            EventType = "added"
	EventResumed          EventType = "resumed"
	EventCleaned          EventType = "cleaned"
)

// ParentOptions defines options for job parent relationships.
type ParentOptions struct {
	ID    string `json:"id" msgpack:"id"`
	Queue string `json:"queue" msgpack:"queue"`
}

// KeepJobs specifies how many completed/failed jobs to keep.
// Count 0 removes all jobs immediately, -1 keeps all jobs; Age (seconds)
// prunes jobs older than the given age. An age-only policy (Age set, Count 0)
// serializes without a count key, like upstream {age: N}: the Lua side then
// keeps jobs (pruning by age) instead of reading count 0 as "remove all".
type KeepJobs struct {
	Age   int `json:"age,omitempty" msgpack:"age,omitempty"`
	Count int `json:"count" msgpack:"count"`
}

// wireMap is the upstream-compatible encoding of the retention policy.
func (k KeepJobs) wireMap() map[string]int {
	m := make(map[string]int, 2)
	if k.Age > 0 {
		m["age"] = k.Age
	}
	if k.Count != 0 || k.Age == 0 {
		m["count"] = k.Count
	}
	return m
}

// MarshalJSON emits the upstream encoding (see wireMap).
func (k KeepJobs) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.wireMap())
}

// EncodeMsgpack emits the upstream encoding (see wireMap).
func (k KeepJobs) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(k.wireMap())
}

// UnmarshalJSON normalizes bool, int, or object into KeepJobs.
func (k *KeepJobs) UnmarshalJSON(b []byte) error {
	var boolVal bool
	if err := json.Unmarshal(b, &boolVal); err == nil {
		if boolVal {
			k.Count = 0
		} else {
			k.Count = -1
		}
		return nil
	}
	var intVal int
	if err := json.Unmarshal(b, &intVal); err == nil {
		k.Count = intVal
		return nil
	}
	type alias KeepJobs
	var tmp alias
	if err := json.Unmarshal(b, &tmp); err == nil {
		*k = KeepJobs(tmp)
		return nil
	}
	k.Count = -1
	return nil
}

// BackoffOptions configures retry backoff strategy for a job.
type BackoffOptions struct {
	Type  string `json:"type" msgpack:"type"`
	Delay int    `json:"delay" msgpack:"delay"`
}

// JobRepeatOptions defines options for configuring repeatable jobs.
// Date fields are Unix epoch milliseconds, matching the BullMQ wire format.
type JobRepeatOptions struct {
	CurrentDate  int64  `json:"currentDate,omitempty" msgpack:"currentDate,omitempty"`
	StartDate    int64  `json:"startDate,omitempty" msgpack:"startDate,omitempty"`
	EndDate      int64  `json:"endDate,omitempty" msgpack:"endDate,omitempty"`
	UTC          bool   `json:"utc,omitempty" msgpack:"utc,omitempty"`
	TZ           string `json:"tz,omitempty" msgpack:"tz,omitempty"`
	NthDayOfWeek int    `json:"nthDayOfWeek,omitempty" msgpack:"nthDayOfWeek,omitempty"`
	Pattern      string `json:"pattern,omitempty" msgpack:"pattern,omitempty"`
	Limit        int    `json:"limit,omitempty" msgpack:"limit,omitempty"`
	Every        int    `json:"every,omitempty" msgpack:"every,omitempty"`
	Immediately  bool   `json:"immediately,omitempty" msgpack:"immediately,omitempty"`
	Count        int    `json:"count,omitempty" msgpack:"count,omitempty"`
	Offset       int64  `json:"offset,omitempty" msgpack:"offset,omitempty"`
	JobID        string `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
	// legacyPrevMillis is populated only while reading jobs written by the
	// pre-redesign Go format, which nested prevMillis under repeat.
	legacyPrevMillis int64
}

// UnmarshalJSON accepts dates as epoch millis (the wire format) or as
// ISO-8601 strings, which JS producers can store when given Date objects.
func (o *JobRepeatOptions) UnmarshalJSON(b []byte) error {
	type alias JobRepeatOptions
	aux := struct {
		CurrentDate json.RawMessage `json:"currentDate,omitempty"`
		StartDate   json.RawMessage `json:"startDate,omitempty"`
		EndDate     json.RawMessage `json:"endDate,omitempty"`
		PrevMillis  json.RawMessage `json:"prevMillis,omitempty"`
		*alias
	}{alias: (*alias)(o)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var err error
	if o.CurrentDate, err = parseEpochMillis(aux.CurrentDate); err != nil {
		return fmt.Errorf("repeat currentDate: %w", err)
	}
	if o.StartDate, err = parseEpochMillis(aux.StartDate); err != nil {
		return fmt.Errorf("repeat startDate: %w", err)
	}
	if o.EndDate, err = parseEpochMillis(aux.EndDate); err != nil {
		return fmt.Errorf("repeat endDate: %w", err)
	}
	if o.legacyPrevMillis, err = parseEpochMillis(aux.PrevMillis); err != nil {
		return fmt.Errorf("repeat prevMillis: %w", err)
	}
	return nil
}

func parseEpochMillis(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f), nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("invalid date value %s", raw)
}

// JobOptions defines options for configuring a job.
type JobOptions struct {
	Priority         int       `json:"priority,omitempty" msgpack:"priority,omitempty"`
	RemoveOnComplete *KeepJobs `json:"removeOnComplete,omitempty" msgpack:"removeOnComplete,omitempty"`
	RemoveOnFail     *KeepJobs `json:"removeOnFail,omitempty" msgpack:"removeOnFail,omitempty"`
	// Attempts and Delay are always serialized: upstream stores
	// { attempts: 0, delay: 0 } defaults on every job.
	Attempts  int    `json:"attempts" msgpack:"attempts"`
	Delay     int    `json:"delay" msgpack:"delay"`
	Timestamp int64  `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
	Lifo      bool   `json:"lifo,omitempty" msgpack:"lifo,omitempty"`
	JobID     string `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
	// RepeatJobKey is carried on the job (rjk hash field) but stripped from
	// the serialized opts, like upstream's Job constructor destructuring.
	RepeatJobKey string            `json:"repeatJobKey,omitempty" msgpack:"repeatJobKey,omitempty"`
	Repeat       *JobRepeatOptions `json:"repeat,omitempty" msgpack:"repeat,omitempty"`
	// FailParentOnFailure, RemoveDependencyOnFailure and KeepLogs use
	// upstream's shortened Redis keys (RedisJobOptions: fpof/rdof/kl).
	FailParentOnFailure       bool           `json:"fpof,omitempty" msgpack:"fpof,omitempty"`
	Parent                    *ParentOptions `json:"parent,omitempty" msgpack:"parent,omitempty"`
	RemoveDependencyOnFailure bool           `json:"rdof,omitempty" msgpack:"rdof,omitempty"`
	// KeepLogs caps the number of log entries kept per job (0 = unlimited).
	KeepLogs int `json:"kl,omitempty" msgpack:"kl,omitempty"`
	// StackTraceLimit caps the stacktrace entries kept on failures (0 = unlimited).
	StackTraceLimit int `json:"stackTraceLimit,omitempty" msgpack:"stackTraceLimit,omitempty"`
	// SizeLimit rejects jobs whose JSON data exceeds this many bytes (0 = unlimited).
	SizeLimit int             `json:"sizeLimit,omitempty" msgpack:"sizeLimit,omitempty"`
	Backoff   *BackoffOptions `json:"backoff,omitempty" msgpack:"backoff,omitempty"`
	// PrevMillis is the slot of the previous repeatable iteration (epoch ms).
	// Upstream stores it top-level on the job opts, not inside repeat.
	PrevMillis   int64 `json:"prevMillis,omitempty" msgpack:"prevMillis,omitempty"`
	WaitChildren bool  `json:"-" msgpack:"-"`
}

// UnmarshalJSON accepts both the current BullMQ option keys and the long keys
// emitted by pre-redesign Go releases. New jobs continue to marshal only the
// upstream-compatible short keys.
func (o *JobOptions) UnmarshalJSON(b []byte) error {
	type alias JobOptions
	aux := struct {
		LegacyFailParentOnFailure       bool `json:"failParentOnFailure,omitempty"`
		LegacyRemoveDependencyOnFailure bool `json:"removeDependencyOnFailure,omitempty"`
		*alias
	}{alias: (*alias)(o)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if !o.FailParentOnFailure {
		o.FailParentOnFailure = aux.LegacyFailParentOnFailure
	}
	if !o.RemoveDependencyOnFailure {
		o.RemoveDependencyOnFailure = aux.LegacyRemoveDependencyOnFailure
	}
	if o.PrevMillis == 0 && o.Repeat != nil {
		o.PrevMillis = o.Repeat.legacyPrevMillis
	}
	return nil
}

// rawJob is the internal untyped job representation used for Redis wire-format operations.
// All fields are unexported; the public API uses Job[D] which wraps rawJob.
type rawJob struct {
	id           string
	name         string
	data         any // raw data (JSON string from Redis, or pre-marshal Go value)
	opts         JobOptions
	parentKey    string
	timestamp    int64
	progress     any
	delay        int
	finishedOn   time.Time
	processedOn  time.Time
	repeatJobKey string
	failedReason string
	stacktrace   []string
	attemptsMade int
	returnValue  any
	token        string
	discarded    bool

	client    redis.Cmdable
	keyPrefix string
}

func (j *rawJob) setJobContext(client redis.Cmdable, prefix string) {
	j.client = client
	j.keyPrefix = prefix
}

func (j *rawJob) hasJobContext() bool {
	return j.client != nil && j.keyPrefix != ""
}

// Job is the public typed wrapper around an internal rawJob.
// D is the type of the job's data payload.
type Job[D any] struct {
	raw  *rawJob
	data D
}

// wrapRawJob deserializes a rawJob's data field into type D and returns a typed Job[D].
func wrapRawJob[D any](raw *rawJob) (*Job[D], error) {
	var data D
	switch v := raw.data.(type) {
	case string:
		if v != "" {
			if err := json.Unmarshal([]byte(v), &data); err != nil {
				return nil, fmt.Errorf("failed to unmarshal job data: %w", err)
			}
		}
	case []byte:
		if len(v) > 0 {
			if err := json.Unmarshal(v, &data); err != nil {
				return nil, fmt.Errorf("failed to unmarshal job data: %w", err)
			}
		}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal job data for type conversion: %w", err)
		}
		if err := json.Unmarshal(b, &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal job data: %w", err)
		}
	}
	return &Job[D]{raw: raw, data: data}, nil
}

// --- Accessor methods ---

// ID returns the job's unique identifier.
func (j *Job[D]) ID() string { return j.raw.id }

// Name returns the job's name.
func (j *Job[D]) Name() string { return j.raw.name }

// Data returns the job's typed data payload.
func (j *Job[D]) Data() D { return j.data }

// Options returns the options the job was created with.
func (j *Job[D]) Options() JobOptions { return j.raw.opts }

// ParentKey returns the Redis key of the job's parent, if any.
func (j *Job[D]) ParentKey() string { return j.raw.parentKey }

// Timestamp returns the job's creation time.
func (j *Job[D]) Timestamp() time.Time { return time.UnixMilli(j.raw.timestamp) }

// Progress returns the job's last reported progress value. BullMQ progress can
// be either a number or an arbitrary JSON object.
func (j *Job[D]) Progress() any { return j.raw.progress }

// Delay returns the job's initial processing delay.
func (j *Job[D]) Delay() time.Duration { return time.Duration(j.raw.delay) * time.Millisecond }

// FinishedOn returns when the job finished (zero if not finished).
func (j *Job[D]) FinishedOn() time.Time { return j.raw.finishedOn }

// ProcessedOn returns when processing started (zero if not started).
func (j *Job[D]) ProcessedOn() time.Time { return j.raw.processedOn }

// RepeatJobKey returns the repeatable-job key for repeat iterations.
func (j *Job[D]) RepeatJobKey() string { return j.raw.repeatJobKey }

// FailedReason returns the error text of the job's last failure.
func (j *Job[D]) FailedReason() string { return j.raw.failedReason }

// AttemptsMade returns how many processing attempts the job has made.
func (j *Job[D]) AttemptsMade() int { return j.raw.attemptsMade }

// ReturnValue returns the job's processor result as decoded JSON.
func (j *Job[D]) ReturnValue() any { return j.raw.returnValue }

// Token returns the lock token the job was fetched with.
func (j *Job[D]) Token() string { return j.raw.token }

// ReturnValueAs converts j's return value into R through a JSON round-trip,
// giving typed access to a processor result that was read back from Redis.
func ReturnValueAs[R any, D any](j *Job[D]) (R, error) {
	var out R
	b, err := json.Marshal(j.ReturnValue())
	if err != nil {
		return out, fmt.Errorf("failed to marshal return value: %w", err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("failed to unmarshal return value: %w", err)
	}
	return out, nil
}

// --- Internal job loading functions ---

// jobFromId loads a job from Redis by its ID.
// Returns ErrJobNotFound if the job does not exist.
func jobFromId(ctx context.Context, client redis.Cmdable, queueKey string, jobId string) (rawJob, error) {
	jobData, err := client.HGetAll(ctx, queueKey+jobId).Result()
	if err != nil {
		return rawJob{}, err
	}

	if len(jobData) == 0 {
		return rawJob{}, ErrJobNotFound
	}

	jobDataInterface := make(map[string]any, len(jobData))
	for k, v := range jobData {
		jobDataInterface[k] = v
	}

	job, err := jobFromJson(jobDataInterface)
	if err != nil {
		return rawJob{}, err
	}
	// The job hash has no "id" field; the ID is only encoded in the key.
	job.id = jobId

	return job, nil
}

// jobFromJson creates a rawJob from a map of string key-value pairs.
func jobFromJson(jobData map[string]any) (rawJob, error) {
	dataVal := jobData["data"]
	optsStr, optsOk := jobData["opts"].(string)
	nameStr, _ := jobData["name"].(string)
	idStr, _ := jobData["id"].(string)

	var opts JobOptions
	var err error
	if optsOk && optsStr != "" {
		opts, err = jobOptsFromJson(optsStr)
		if err != nil {
			return rawJob{}, fmt.Errorf("failed to parse job options JSON string: %w", err)
		}
	}

	job := rawJob{
		name:     nameStr,
		data:     dataVal,
		opts:     opts,
		id:       idStr,
		progress: 0,
	}

	parseStrToInt := func(key string) int {
		if strVal, ok := jobData[key].(string); ok {
			if val, err := strconv.Atoi(strVal); err == nil {
				return val
			}
		}
		return 0
	}
	parseStrToInt64 := func(key string) int64 {
		if strVal, ok := jobData[key].(string); ok {
			if val, err := strconv.ParseInt(strVal, 10, 64); err == nil {
				return val
			}
		}
		return 0
	}
	parseStrToTime := func(key string) time.Time {
		tsVal := parseStrToInt64(key)
		if tsVal > 0 {
			return time.UnixMilli(tsVal)
		}
		return time.Time{}
	}

	job.timestamp = parseStrToInt64("timestamp")
	if progress, ok := jobData["progress"]; ok {
		job.progress = parseProgressValue(progress)
	}
	job.delay = parseStrToInt("delay")
	job.finishedOn = parseStrToTime("finishedOn")
	job.processedOn = parseStrToTime("processedOn")
	if rjkStr, ok := jobData["rjk"].(string); ok {
		job.repeatJobKey = rjkStr
	}
	if frStr, ok := jobData["failedReason"].(string); ok {
		job.failedReason = frStr
	}
	if stStr, ok := jobData["stacktrace"].(string); ok && stStr != "" {
		_ = json.Unmarshal([]byte(stStr), &job.stacktrace)
	}
	job.attemptsMade = parseStrToInt("attemptsMade")
	if pkStr, ok := jobData["parentKey"].(string); ok {
		job.parentKey = pkStr
	}

	if retVal, ok := jobData["returnvalue"]; ok {
		if retStr, okStr := retVal.(string); okStr {
			var parsedRet any
			if err := json.Unmarshal([]byte(retStr), &parsedRet); err == nil {
				job.returnValue = parsedRet
			} else {
				job.returnValue = retStr
			}
		} else {
			job.returnValue = retVal
		}
	}

	return job, nil
}

func parseProgressValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err == nil {
		return parsed
	}
	return s
}

// jobOptsFromJson unmarshals a JSON string into JobOptions.
func jobOptsFromJson(rawOpts string) (JobOptions, error) {
	var jobOpts JobOptions
	if err := json.Unmarshal([]byte(rawOpts), &jobOpts); err != nil {
		return jobOpts, fmt.Errorf("failed to unmarshal job opts: %w", err)
	}
	return jobOpts, nil
}

// finishedErrors maps moveToFinished return codes to errors, mirroring
// upstream Scripts.finishedErrors.
func finishedErrors(code int64, jobId string, command string) error {
	switch code {
	case -1:
		return fmt.Errorf("%w %s while executing %s", ErrJobNotFound, jobId, command)
	case -2:
		return fmt.Errorf("%w %s while executing %s", ErrMissingLock, jobId, command)
	case -3:
		return fmt.Errorf("%w: job %s is not in the active state while executing %s", ErrJobNotInState, jobId, command)
	case -4:
		return fmt.Errorf("bullmq: job %s has pending dependencies while executing %s", jobId, command)
	case -5:
		return fmt.Errorf("bullmq: missing key for parent job %s while executing %s", jobId, command)
	case -6:
		return fmt.Errorf("%w: lock for job %s is not owned by this client while executing %s", ErrMissingLock, jobId, command)
	default:
		return fmt.Errorf("bullmq: unknown code %d error for job %s while executing %s", code, jobId, command)
	}
}

// prepareStacktrace updates the in-memory failedReason/stacktrace for a new
// failure and returns the JSON stacktrace to persist, mirroring upstream
// Job.saveStacktrace's bookkeeping (Go errors carry no stack, so the error
// text is recorded; upstream keeps the first stackTraceLimit entries).
func (j *rawJob) prepareStacktrace(jobErr error) (string, error) {
	j.failedReason = jobErr.Error()
	j.stacktrace = append(j.stacktrace, jobErr.Error())
	if limit := j.opts.StackTraceLimit; limit > 0 && len(j.stacktrace) > limit {
		j.stacktrace = j.stacktrace[:limit]
	}
	st, err := json.Marshal(j.stacktrace)
	if err != nil {
		return "", err
	}
	return string(st), nil
}

// runFailureMulti atomically persists stacktrace/failedReason and applies the
// state move in one MULTI, like upstream job.moveToFailed's single multi.
func runFailureMulti(ctx context.Context, client redis.Cmdable, jobKey string, stacktrace string, failedReason string, move *redis.Script, keys []string, args []any) (any, error) {
	// Load both scripts first: EVALSHA inside MULTI cannot fall back on NOSCRIPT.
	if err := lua.SaveStacktraceScript.Load(ctx, client).Err(); err != nil {
		return nil, err
	}
	if err := move.Load(ctx, client).Err(); err != nil {
		return nil, err
	}
	pipe := client.TxPipeline()
	lua.SaveStacktraceScript.Run(ctx, pipe, []string{jobKey}, stacktrace, failedReason)
	cmd := move.Run(ctx, pipe, keys, args...)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return cmd.Result()
}

// jobMoveToFailed moves a job to the 'failed' set in Redis, persisting the
// stacktrace atomically with the move. The job's own RemoveOnFail wins over
// the worker-level default held by scripts.
func jobMoveToFailed(ctx context.Context, s *scripts, job *rawJob, stacktrace string, token string, fetchNext bool, lockDurationMs int, maxMetricsSize string) error {
	keys, args, scriptErr := s.moveToFailedArgs(job, job.failedReason, job.opts.RemoveOnFail, token, fetchNext, lockDurationMs, maxMetricsSize)
	if scriptErr != nil {
		return fmt.Errorf("error preparing move to failed args for job %s: %w", job.id, scriptErr)
	}
	result, luaErr := runFailureMulti(ctx, s.redisClient, s.keyPrefix+job.id, stacktrace, job.failedReason, lua.MoveToFinishedScript, keys, args)
	if luaErr != nil {
		return fmt.Errorf("error executing move to failed via Lua for job %s: %w", job.id, luaErr)
	}
	if code := moveToFinishedCode(result); code < 0 {
		return finishedErrors(code, job.id, "moveToFailed")
	}
	return nil
}

func newJob(name string, data any, opts JobOptions) (rawJob, error) {
	op := setOpts(opts)
	if op.SizeLimit > 0 {
		if s, ok := data.(string); ok && len(s) > op.SizeLimit {
			return rawJob{}, fmt.Errorf("bullmq: the size of job %s exceeds the limit %d bytes", name, op.SizeLimit)
		}
	}
	// The job timestamp defaults to now, but opts.timestamp is only stored
	// when the caller supplied one, like upstream.
	ts := op.Timestamp
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	curJob := rawJob{
		opts:         op,
		name:         name,
		data:         data,
		progress:     0,
		delay:        op.Delay,
		timestamp:    ts,
		repeatJobKey: op.RepeatJobKey,
		attemptsMade: 0,
	}
	return curJob, nil
}

func setOpts(opts JobOptions) JobOptions {
	op := opts
	if op.Delay < 0 {
		op.Delay = 0
	}
	// Attempts 0 is stored as-is like upstream; the retry comparison
	// attemptsMade < attempts treats 0 and 1 identically (one attempt).
	return op
}
