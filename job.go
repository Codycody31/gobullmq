package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
)

const (
	_DEFAULT_JOB_NAME = "__default__"
)

// JobState represents the state of a job in the queue.
type JobState string

const (
	JobStateCompleted       JobState = "completed"
	JobStateWaiting         JobState = "wait"
	JobStateActive          JobState = "active"
	JobStatePaused          JobState = "paused"
	JobStatePrioritized     JobState = "prioritized"
	JobStateDelayed         JobState = "delayed"
	JobStateFailed          JobState = "failed"
	JobStateWaitingChildren JobState = "waiting-children"
)

// EventType represents a queue event type emitted via Redis streams.
type EventType string

const (
	EventCompleted        EventType = "completed"
	EventWaiting          EventType = "wait"
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

// ParentOpts defines options for job parent relationships.
type ParentOpts struct {
	ID    string `json:"id" msgpack:"id"`
	Queue string `json:"queue" msgpack:"queue"`
}

// KeepJobs specifies how many completed/failed jobs to keep.
type KeepJobs struct {
	Age   int `json:"age,omitempty" msgpack:"age,omitempty"`
	Count int `json:"count,omitempty" msgpack:"count,omitempty"`
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
type JobRepeatOptions struct {
	CurrentDate  *time.Time `json:"currentDate,omitempty" msgpack:"currentDate,omitempty"`
	StartDate    *time.Time `json:"startDate,omitempty" msgpack:"startDate,omitempty"`
	EndDate      *time.Time `json:"endDate,omitempty" msgpack:"endDate,omitempty"`
	UTC          bool       `json:"utc,omitempty" msgpack:"utc,omitempty"`
	TZ           string     `json:"tz,omitempty" msgpack:"tz,omitempty"`
	NthDayOfWeek int        `json:"nthDayOfWeek,omitempty" msgpack:"nthDayOfWeek,omitempty"`
	Pattern      string     `json:"pattern,omitempty" msgpack:"pattern,omitempty"`
	Limit        int        `json:"limit,omitempty" msgpack:"limit,omitempty"`
	Every        int        `json:"every,omitempty" msgpack:"every,omitempty"`
	Immediately  bool       `json:"immediately,omitempty" msgpack:"immediately,omitempty"`
	Count        int        `json:"count,omitempty" msgpack:"count,omitempty"`
	PrevMillis   int        `json:"prevMillis,omitempty" msgpack:"prevMillis,omitempty"`
	Offset       int        `json:"offset,omitempty" msgpack:"offset,omitempty"`
	JobID        string     `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
}

// JobOptions defines options for configuring a job.
type JobOptions struct {
	Priority                  int               `json:"priority,omitempty" msgpack:"priority,omitempty"`
	RemoveOnComplete          *KeepJobs         `json:"removeOnComplete,omitempty" msgpack:"removeOnComplete,omitempty"`
	RemoveOnFail              *KeepJobs         `json:"removeOnFail,omitempty" msgpack:"removeOnFail,omitempty"`
	Attempts                  int               `json:"attempts,omitempty" msgpack:"attempts,omitempty"`
	Delay                     int               `json:"delay,omitempty" msgpack:"delay,omitempty"`
	Timestamp                 int64             `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
	Lifo                      bool              `json:"lifo,omitempty" msgpack:"lifo,omitempty"`
	JobID                     string            `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
	RepeatJobKey              string            `json:"repeatJobKey,omitempty" msgpack:"repeatJobKey,omitempty"`
	Token                     string            `json:"token,omitempty" msgpack:"token,omitempty"`
	Repeat                    *JobRepeatOptions `json:"repeat,omitempty" msgpack:"repeat,omitempty"`
	FailParentOnFailure       bool              `json:"failParentOnFailure,omitempty" msgpack:"failParentOnFailure,omitempty"`
	Parent                    *ParentOpts       `json:"parent,omitempty" msgpack:"parent,omitempty"`
	RemoveDependencyOnFailure bool              `json:"removeDependencyOnFailure,omitempty" msgpack:"removeDependencyOnFailure,omitempty"`
	Backoff                   *BackoffOptions   `json:"backoff,omitempty" msgpack:"backoff,omitempty"`
	WaitChildren              bool              `json:"-" msgpack:"-"`
}

// rawJob is the internal untyped job representation used for Redis wire-format operations.
// All fields are unexported; the public API uses Job[D] which wraps rawJob.
type rawJob struct {
	id           string
	name         string
	data         interface{} // raw data (JSON string from Redis, or pre-marshal Go value)
	opts         JobOptions
	optsByJSON   []byte
	parentKey    string
	timestamp    int64
	progress     int
	delay        int
	finishedOn   time.Time
	processedOn  time.Time
	repeatJobKey string
	failedReason string
	attemptsMade int
	returnValue  interface{}
	token        string

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

func (j *rawJob) toJSONData() error {
	data, err := json.Marshal(j.opts)
	if err != nil {
		return err
	}
	j.optsByJSON = data
	return nil
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

func (j *Job[D]) ID() string              { return j.raw.id }
func (j *Job[D]) Name() string            { return j.raw.name }
func (j *Job[D]) Data() D                 { return j.data }
func (j *Job[D]) Opts() JobOptions        { return j.raw.opts }
func (j *Job[D]) ParentKey() string        { return j.raw.parentKey }
func (j *Job[D]) Timestamp() int64        { return j.raw.timestamp }
func (j *Job[D]) Progress() int           { return j.raw.progress }
func (j *Job[D]) Delay() int              { return j.raw.delay }
func (j *Job[D]) FinishedOn() time.Time   { return j.raw.finishedOn }
func (j *Job[D]) ProcessedOn() time.Time  { return j.raw.processedOn }
func (j *Job[D]) RepeatJobKey() string     { return j.raw.repeatJobKey }
func (j *Job[D]) FailedReason() string     { return j.raw.failedReason }
func (j *Job[D]) AttemptsMade() int        { return j.raw.attemptsMade }
func (j *Job[D]) ReturnValue() interface{} { return j.raw.returnValue }
func (j *Job[D]) Token() string            { return j.raw.token }

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

	jobDataInterface := make(map[string]interface{}, len(jobData))
	for k, v := range jobData {
		jobDataInterface[k] = v
	}

	job, err := jobFromJson(jobDataInterface)
	if err != nil {
		return rawJob{}, err
	}

	return job, nil
}

// jobFromJson creates a rawJob from a map of string key-value pairs.
func jobFromJson(jobData map[string]interface{}) (rawJob, error) {
	dataVal, _ := jobData["data"]
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
		name: nameStr,
		data: dataVal,
		opts: opts,
		id:   idStr,
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
	job.progress = parseStrToInt("progress")
	job.delay = parseStrToInt("delay")
	job.finishedOn = parseStrToTime("finishedOn")
	job.processedOn = parseStrToTime("processedOn")
	if rjkStr, ok := jobData["rjk"].(string); ok {
		job.repeatJobKey = rjkStr
	}
	if frStr, ok := jobData["failedReason"].(string); ok {
		job.failedReason = frStr
	}
	job.attemptsMade = parseStrToInt("attemptsMade")
	if pkStr, ok := jobData["parentKey"].(string); ok {
		job.parentKey = pkStr
	}

	if retVal, ok := jobData["returnvalue"]; ok {
		if retStr, okStr := retVal.(string); okStr {
			var parsedRet interface{}
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

// jobOptsFromJson unmarshals a JSON string into JobOptions.
func jobOptsFromJson(rawOpts string) (JobOptions, error) {
	var jobOpts JobOptions
	if err := json.Unmarshal([]byte(rawOpts), &jobOpts); err != nil {
		return jobOpts, fmt.Errorf("failed to unmarshal job opts: %w", err)
	}
	return jobOpts, nil
}

// jobMoveToFailed moves a job to the 'failed' set in Redis.
func jobMoveToFailed(ctx context.Context, s *scripts, job *rawJob, err error, token string, removeOnFailed KeepJobs, fetchNext bool, lockDurationMs int, maxMetricsSize string) error {
	job.failedReason = err.Error()
	keys, args, scriptErr := s.moveToFailedArgs(job, job.failedReason, removeOnFailed, token, fetchNext, lockDurationMs, maxMetricsSize)
	if scriptErr != nil {
		return fmt.Errorf("error preparing move to failed args for job %s: %w", job.id, scriptErr)
	}
	_, luaErr := lua.MoveToFinished(ctx, s.redisClient, keys, args...)
	if luaErr != nil {
		return fmt.Errorf("error executing move to failed via Lua for job %s: %w", job.id, luaErr)
	}
	return nil
}

func newJob(name string, data interface{}, opts JobOptions) (rawJob, error) {
	op := setOpts(opts)
	if name == "" {
		name = _DEFAULT_JOB_NAME
	}
	curJob := rawJob{
		opts:         op,
		name:         name,
		data:         data,
		progress:     0,
		delay:        op.Delay,
		timestamp:    op.Timestamp,
		attemptsMade: 0,
	}
	err := curJob.toJSONData()
	if err != nil {
		return curJob, err
	}
	return curJob, nil
}

func setOpts(opts JobOptions) JobOptions {
	op := opts
	if op.Delay < 0 {
		op.Delay = 0
	}
	if op.Attempts == 0 {
		op.Attempts = 1
	}
	if op.Timestamp == 0 {
		op.Timestamp = time.Now().UnixMilli()
	}
	return op
}
