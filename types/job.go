package types

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisJobOptions defines options for Redis jobs.
type RedisJobOptions struct {
	fpof bool // If true, moves parent to failed.
	kl   int  // Maximum amount of log entries that will be preserved
	rdof bool // If true, removes the job from its parent dependencies when it fails after all attempts.
}

// ParentOpts defines options for job parent relationships.
type ParentOpts struct {
	Id    string `json:"id" msgpack:"id"`       // The job ID of the parent.
	Queue string `json:"queue" msgpack:"queue"` // The queue prefix (e.g., "bull:myParentQueue") of the parent.
}

// KeepJobs specifies how many completed/failed jobs to keep.
type KeepJobs struct {
	Age   int `json:"age,omitempty" msgpack:"age,omitempty"`     // Maximum age in seconds for job to be kept.
	Count int `json:"count,omitempty" msgpack:"count,omitempty"` // Maximum count of jobs to be kept.
}

// JobData represents the data associated with a job.
type JobData interface{}

// JobOptionFunc defines a function type for setting job options.
type JobOptionFunc func(*JobOptions)

// JobOptions defines options for configuring a job.
type JobOptions struct {
	Priority         int       `json:"priority,omitempty" msgpack:"priority,omitempty"`
	RemoveOnComplete *KeepJobs `json:"removeOnComplete,omitempty" msgpack:"removeOnComplete,omitempty"`
	RemoveOnFail     *KeepJobs `json:"removeOnFail,omitempty" msgpack:"removeOnFail,omitempty"`
	Attempts         int       `json:"attempts,omitempty" msgpack:"attempts,omitempty"`
	Delay            int       `json:"delay,omitempty" msgpack:"delay,omitempty"`
	TimeStamp        int64     `json:"timestamp,omitempty" msgpack:"timestamp,omitempty"`
	Lifo             bool      `json:"lifo,omitempty" msgpack:"lifo,omitempty"`
	JobId            string    `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
	RepeatJobKey     string    `json:"repeatJobKey,omitempty" msgpack:"repeatJobKey,omitempty"`
	Token            string    `json:"token,omitempty" msgpack:"token,omitempty"` // The token used for locking this job.

	Repeat *JobRepeatOptions `json:"repeat,omitempty" msgpack:"repeat,omitempty"`

	// Added fields
	FailParentOnFailure       bool        `json:"failParentOnFailure,omitempty" msgpack:"failParentOnFailure,omitempty"`
	Parent                    *ParentOpts `json:"parent,omitempty" msgpack:"parent,omitempty"`
	RemoveDependencyOnFailure bool        `json:"removeDependencyOnFailure,omitempty" msgpack:"removeDependencyOnFailure,omitempty"`
}

// JobRepeatOptions defines options for configuring repeatable jobs.
type JobRepeatOptions struct {
	CurrentDate  *time.Time `json:"currentDate,omitempty" msgpack:"currentDate,omitempty"`
	StartDate    *time.Time `json:"startDate,omitempty" msgpack:"startDate,omitempty"`
	EndDate      *time.Time `json:"endDate,omitempty" msgpack:"endDate,omitempty"`
	UTC          bool       `json:"utc,omitempty" msgpack:"utc,omitempty"`
	TZ           string     `json:"tz,omitempty" msgpack:"tz,omitempty"`
	NthDayOfWeek int        `json:"nthDayOfWeek,omitempty" msgpack:"nthDayOfWeek,omitempty"`

	Pattern     string `json:"pattern,omitempty" msgpack:"pattern,omitempty"`         // A repeat pattern
	Limit       int    `json:"limit,omitempty" msgpack:"limit,omitempty"`             // Number of times the job should repeat at max.
	Every       int    `json:"every,omitempty" msgpack:"every,omitempty"`             // Repeat after this amount of milliseconds (`pattern` setting cannot be used together with this setting.)
	Immediately bool   `json:"immediately,omitempty" msgpack:"immediately,omitempty"` // Repeated job should start right now (work only with every settings)
	Count       int    `json:"count,omitempty" msgpack:"count,omitempty"`             // The start value for the repeat iteration count.
	PrevMillis  int    `json:"prevMillis,omitempty" msgpack:"prevMillis,omitempty"`
	Offset      int    `json:"offset,omitempty" msgpack:"offset,omitempty"`
	JobId       string `json:"jobId,omitempty" msgpack:"jobId,omitempty"`
}

// Job represents a job with its associated data and options.
type Job struct {
	Name           string
	Id             string
	Data           JobData
	Opts           JobOptions
	OptsByJson     []byte
	ParentKey      string
	TimeStamp      int64
	Progress       int
	Delay          int
	DelayTimeStamp int64
	FinishedOn     time.Time
	ProcessedOn    time.Time
	RepeatJobKey   string
	FailedReason   string
	AttemptsMade   int
	Returnvalue    interface{}
	Token          string
}

// ToJsonData marshals the job options to JSON.
func (job *Job) ToJsonData() error {
	data, err := json.Marshal(job.Opts)
	if err != nil {
		return err
	}
	job.OptsByJson = data
	return err
}

func (j *Job) MoveToCompleted(ctx context.Context, client redis.Cmdable, queueKey string, result interface{}, token string, fetchNext bool) ([]interface{}, error) {
	j.Returnvalue = result

	stringifiedReturnValue, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	//   const result = await this.scripts.moveToFinished(this.id, args);
	//   this.finishedOn = args[14] as number;

	//   return result;
	return []interface{}{stringifiedReturnValue, j.FinishedOn.Unix()}, nil
}
