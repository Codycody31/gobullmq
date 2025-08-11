package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/types"
)

const (
	_DEFAULT_JOB_NAME = "__default__"
)

func JobFromId(ctx context.Context, client redis.Cmdable, queueKey string, jobId string) (types.Job, error) {
	jobData, err := client.HGetAll(ctx, queueKey+jobId).Result()
	if err != nil {
		return types.Job{}, err
	}

	if len(jobData) == 0 {
		return types.Job{}, nil
	}

	// Convert map[string]string from HGetAll to map[string]interface{}
	jobDataInterface := make(map[string]interface{}, len(jobData))
	for k, v := range jobData {
		jobDataInterface[k] = v
	}

	job, err := JobFromJson(jobDataInterface)
	if err != nil {
		return types.Job{}, err
	}

	return job, nil
}

func JobFromJson(jobData map[string]interface{}) (types.Job, error) {
	// Get raw strings/values, handle missing keys and type assertions gracefully
	dataVal, _ := jobData["data"]               // Data might be nil or string initially
	optsStr, optsOk := jobData["opts"].(string) // Opts should be a JSON string
	nameStr, _ := jobData["name"].(string)
	idStr, _ := jobData["id"].(string)

	// If opts string is missing or empty, create default opts
	var opts types.JobOptions
	var err error
	if optsOk && optsStr != "" {
		opts, err = JobOptsFromJson(optsStr)
		if err != nil {
			return types.Job{}, fmt.Errorf("failed to parse job options JSON string: %w", err)
		}
	} else {
		// Handle case where opts might be missing - apply defaults?
		// Or should this be an error? For now, use default struct.
		fmt.Printf("Warning: Job %s missing 'opts' field or it's not a string.\n", idStr)
	}

	job := types.Job{
		Name: nameStr,
		Data: dataVal, // Keep data as interface{} for now
		Opts: opts,
		Id:   idStr,
	}

	// Helper for safe string conversion and parsing
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
			return time.Unix(tsVal, 0)
		}
		return time.Time{}
	}

	// Parse other fields from strings (with checks)
	job.TimeStamp = parseStrToInt64("timestamp")
	job.Progress = parseStrToInt("progress")
	job.Delay = parseStrToInt("delay")
	job.FinishedOn = parseStrToTime("finishedOn")
	job.ProcessedOn = parseStrToTime("processedOn")
	if rjkStr, ok := jobData["rjk"].(string); ok {
		job.RepeatJobKey = rjkStr
	}
	if frStr, ok := jobData["failedReason"].(string); ok {
		job.FailedReason = frStr
	}
	job.AttemptsMade = parseStrToInt("attemptsMade")
	// Parse parentKey
	if pkStr, ok := jobData["parentKey"].(string); ok {
		job.ParentKey = pkStr
	}

	// Handle return value (might already be interface{}, or json string)
	if retVal, ok := jobData["returnvalue"]; ok {
		if retStr, okStr := retVal.(string); okStr {
			var parsedRet interface{}
			if err := json.Unmarshal([]byte(retStr), &parsedRet); err == nil {
				job.Returnvalue = parsedRet
			} else {
				// If unmarshal fails, store raw string?
				job.Returnvalue = retStr
			}
		} else {
			// If not a string, store the value directly
			job.Returnvalue = retVal
		}
	}

	return job, nil
}

// jobOptsDecodeMap maps JSON keys to struct field names.
var jobOptsDecodeMap = map[string]string{
	"priority":         "Priority",
	"removeOnComplete": "RemoveOnComplete",
	"removeOnFail":     "RemoveOnFail",
	"attempts":         "Attempts",
	"delay":            "Delay",
	"timestamp":        "TimeStamp",
	"lifo":             "Lifo",
	"jobId":            "JobId",
	"repeatJobKey":     "RepeatJobKey",
	"token":            "Token",
	"currentDate":      "CurrentDate",
	"startDate":        "StartDate",
	"endDate":          "EndDate",
	"utc":              "UTC",
	"tz":               "TZ",
	"nthDayOfWeek":     "NthDayOfWeek",
	"pattern":          "Pattern",
	"limit":            "Limit",
	"every":            "Every",
	"immediately":      "Immediately",
	"count":            "Count",
}

func JobOptsFromJson(rawOpts string) (types.JobOptions, error) {
	var tempMap map[string]interface{}
	var jobOpts types.JobOptions

	if err := json.Unmarshal([]byte(rawOpts), &tempMap); err != nil {
		return jobOpts, fmt.Errorf("failed to unmarshal raw opts JSON: %w", err)
	}

	// Helper function for safe type assertion to float64 then int/int64
	parseInt := func(key string) int {
		if val, ok := tempMap[key].(float64); ok {
			return int(val)
		}
		return 0
	}
	parseInt64 := func(key string) int64 {
		if val, ok := tempMap[key].(float64); ok {
			return int64(val)
		}
		return 0
	}
	parseBool := func(key string) bool {
		if val, ok := tempMap[key].(bool); ok {
			return val
		}
		return false
	}
	parseString := func(key string) string {
		if val, ok := tempMap[key].(string); ok {
			return val
		}
		return ""
	}

	// Parse simple fields
	jobOpts.Priority = parseInt("priority")
	jobOpts.Attempts = parseInt("attempts")
	jobOpts.Delay = parseInt("delay")
	jobOpts.TimeStamp = parseInt64("timestamp")
	jobOpts.Lifo = parseBool("lifo") // Ensure Lifo is bool
	jobOpts.JobId = parseString("jobId")
	jobOpts.RepeatJobKey = parseString("repeatJobKey")
	jobOpts.Token = parseString("token")
	jobOpts.FailParentOnFailure = parseBool("failParentOnFailure")

	// Parse nested pointer fields
	parseNested := func(key string, target interface{}) error {
		if rawVal, ok := tempMap[key]; ok && rawVal != nil {
			// Re-marshal the interface{} value back to JSON bytes
			bytes, err := json.Marshal(rawVal)
			if err != nil {
				return fmt.Errorf("failed to re-marshal nested field %s: %w", key, err)
			}
			// Unmarshal the bytes into the target pointer
			if err := json.Unmarshal(bytes, target); err != nil {
				return fmt.Errorf("failed to unmarshal nested field %s: %w", key, err)
			}
		}
		return nil
	}

	var removeOnCompleteOpt types.KeepJobs
	if err := parseNested("removeOnComplete", &removeOnCompleteOpt); err == nil && (removeOnCompleteOpt.Age != 0 || removeOnCompleteOpt.Count != 0) {
		jobOpts.RemoveOnComplete = &removeOnCompleteOpt
	}

	var removeOnFailOpt types.KeepJobs
	if err := parseNested("removeOnFail", &removeOnFailOpt); err == nil && (removeOnFailOpt.Age != 0 || removeOnFailOpt.Count != 0) {
		jobOpts.RemoveOnFail = &removeOnFailOpt
	}

	var repeatOpt types.JobRepeatOptions
	if err := parseNested("repeat", &repeatOpt); err == nil && (repeatOpt.Every != 0 || repeatOpt.Pattern != "") {
		jobOpts.Repeat = &repeatOpt
	}

	var parentOpt types.ParentOpts
	if err := parseNested("parent", &parentOpt); err == nil && parentOpt.Id != "" {
		jobOpts.Parent = &parentOpt
	}

	return jobOpts, nil
}

// TODO: Complete these two key bits of logic, or else we can never finish processing anythings

// JobMoveToFailed moves a job to the 'failed' set in Redis.
// It requires the scripts instance for Redis interaction.
func JobMoveToFailed(s *scripts, job *types.Job, err error, token string, removeOnFailed types.KeepJobs, fetchNext bool) error {
	job.FailedReason = err.Error()

	// TODO: Consider saving stacktrace here if needed, similar to JS version.

	keys, args, scriptErr := s.moveToFailedArgs(job, job.FailedReason, removeOnFailed, token, fetchNext)
	if scriptErr != nil {
		// Consider emitting an error event here or logging
		return fmt.Errorf("error preparing move to failed args for job %s: %w", job.Id, scriptErr)
	}

	// Use the keys and args here to call the appropriate Lua script
	_, luaErr := lua.MoveToFinished(s.redisClient, keys, args...)
	if luaErr != nil {
		// Consider emitting an error event here or logging
		return fmt.Errorf("error executing move to failed via Lua for job %s: %w", job.Id, luaErr)
	}

	// TODO: Consider emitting a 'failed' event here, although the worker currently does this.

	return nil
}

func newJob(name string, data types.JobData, opts types.JobOptions) (types.Job, error) {
	op := setOpts(opts)
	if name == "" {
		name = _DEFAULT_JOB_NAME
	}

	curJob := types.Job{
		Opts:         op,
		Name:         name,
		Data:         data,
		Progress:     0,
		Delay:        op.Delay,
		TimeStamp:    op.TimeStamp,
		AttemptsMade: 0,
	}

	err := curJob.ToJsonData()
	if err != nil {
		return curJob, err
	}

	return curJob, nil
}

func setOpts(opts types.JobOptions) types.JobOptions {
	op := opts

	if opts.Delay < 0 {
		opts.Delay = 0
	}

	if opts.Attempts == 0 {
		op.Attempts = 1
	} else {
		op.Attempts = opts.Attempts
	}

	op.Delay = opts.Delay

	if opts.TimeStamp == 0 {
		op.TimeStamp = time.Now().UnixMilli()
	} else {
		op.TimeStamp = opts.TimeStamp
	}

	return op
}
