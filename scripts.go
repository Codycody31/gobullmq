package gobullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/types"
)

type scripts struct {
	redisClient redis.Cmdable   // Redis client used to interact with the redis server
	ctx         context.Context // Context used to handle the queue events
	keyPrefix   string
}

func newScripts(redisClient redis.Cmdable, ctx context.Context, keyPrefix string) *scripts {
	return &scripts{
		redisClient: redisClient,
		ctx:         ctx,
		keyPrefix:   keyPrefix,
	}
}

func (s *scripts) moveToFailedArgs(job *types.Job, failedReason string, removeOnFailed types.KeepJobs, token string, fetchNext bool) ([]string, []interface{}, error) {
	timestamp := time.Now()
	return s.moveToFinishedArgs(job, failedReason, "failedReason", removeOnFailed, "failed", token, timestamp, fetchNext)
}

// getKeepJobs determines the job retention policy based on provided parameters
func (s *scripts) getKeepJobs(shouldRemove interface{}, workerKeepJobs *types.KeepJobs) types.KeepJobs {
	// If shouldRemove is nil/undefined, use workerKeepJobs or default
	if shouldRemove == nil {
		if workerKeepJobs != nil {
			return *workerKeepJobs
		}
		return types.KeepJobs{Count: -1} // Keep all jobs by default
	}

	// Handle different types of shouldRemove
	switch v := shouldRemove.(type) {
	case types.KeepJobs:
		return v
	case int:
		return types.KeepJobs{Count: v}
	case bool:
		if v {
			return types.KeepJobs{Count: 0} // Remove all (keep none)
		}
		return types.KeepJobs{Count: -1} // Keep all
	default:
		return types.KeepJobs{Count: -1} // Default to keep all
	}
}

func (s *scripts) moveToFinishedArgs(job *types.Job, value string, propValue string, shouldRemove types.KeepJobs, target string, token string, timestamp time.Time, fetchNext bool) ([]string, []interface{}, error) {
	// Build the keys array - equivalent to moveToFinishedKeys in JS
	keys := []string{
		s.keyPrefix + "wait",
		s.keyPrefix + "active",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "events",
		s.keyPrefix + "stalled",
		s.keyPrefix + "limiter",
		s.keyPrefix + "delayed",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
		s.keyPrefix + "pc",
		s.keyPrefix + target,
		s.keyPrefix + job.Id,
		s.keyPrefix + "metrics:" + target,
	}

	// Convert job data to JSON string for the event
	eventData, err := json.Marshal(map[string]interface{}{
		"jobId": job.Id,
		"val":   value,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal event data: %v", err)
	}

	// Prepare options map similar to JS implementation
	opts := map[string]interface{}{
		"token":          token,
		"keepJobs":       shouldRemove,
		"lockDuration":   30000, // TODO: Get from worker options?
		"attempts":       job.Opts.Attempts,
		"attemptsMade":   job.AttemptsMade,
		"maxMetricsSize": "",                                 // TODO: Get from metrics options?
		"fpof":           job.Opts.FailParentOnFailure,       // Use value from job options
		"rdof":           job.Opts.RemoveDependencyOnFailure, // Use value from job options
		"parentKey":      job.ParentKey,                      // Pass parent key
	}

	// Pack options using msgpack
	packedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal options: %v", err)
	}

	// Build the args array
	args := []interface{}{
		job.Id,
		timestamp.Unix(),
		propValue,
		value,
		target,
		string(eventData),
		fetchNext && !false, // Replace with worker.closing check when available
		s.keyPrefix,
		packedOpts,
	}

	return keys, args, nil
}

// UpdateProgress updates the progress of a job
func (s *scripts) updateProgress(jobId string, progress interface{}) error {
	keys := []string{
		s.keyPrefix + jobId,
		s.keyPrefix + jobId + ":events",
	}

	progressJson, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	result, err := lua.UpdateProgress(s.redisClient, keys, jobId, progressJson)
	if err != nil {
		return err
	}

	resultInt64, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type: %T", result)
	}

	if resultInt64 == -1 {
		return fmt.Errorf("job not found")
	}

	return nil
}
