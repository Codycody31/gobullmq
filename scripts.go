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

type scripts struct {
	redisClient redis.Cmdable
	keyPrefix   string
	// Worker-level options consulted by moveToFinished, mirroring upstream
	// where these live on queue.opts: the limiter for the fetch-next path,
	// and the fallback keep policies when the job itself has none.
	limiter          *RateLimiterOptions
	removeOnComplete *KeepJobs
	removeOnFail     *KeepJobs
}

func newScripts(redisClient redis.Cmdable, keyPrefix string) *scripts {
	return &scripts{
		redisClient: redisClient,
		keyPrefix:   keyPrefix,
	}
}

func (s *scripts) moveToFailedArgs(job *rawJob, failedReason string, removeOnFailed *KeepJobs, token string, fetchNext bool, lockDurationMs int, maxMetricsSize string) ([]string, []any, error) {
	timestamp := time.Now()
	return s.moveToFinishedArgs(job, failedReason, "failedReason", removeOnFailed, "failed", token, timestamp, fetchNext, lockDurationMs, maxMetricsSize)
}

// getKeepJobs resolves the retention policy: job-level opts win, then the
// worker-level default, then keep-all, mirroring upstream getKeepJobs.
func (s *scripts) getKeepJobs(shouldRemove *KeepJobs, workerKeepJobs *KeepJobs) KeepJobs {
	if shouldRemove != nil {
		return *shouldRemove
	}
	if workerKeepJobs != nil {
		return *workerKeepJobs
	}
	return KeepJobs{Count: -1} // keep all
}

func (s *scripts) moveToFinishedArgs(job *rawJob, value string, propValue string, shouldRemove *KeepJobs, target string, token string, timestamp time.Time, fetchNext bool, lockDurationMs int, maxMetricsSize string) ([]string, []any, error) {
	// Keys mirror upstream moveToFinishedKeys (scripts.ts).
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
		s.keyPrefix + job.id,
		s.keyPrefix + "metrics:" + target,
	}

	// Event payload for ARGV[6], like upstream JSON.stringify({jobId, val}).
	eventData, err := json.Marshal(map[string]any{
		"jobId": job.id,
		"val":   value,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal event data: %w", err)
	}
	var workerKeepJobs *KeepJobs
	if target == "completed" {
		workerKeepJobs = s.removeOnComplete
	} else {
		workerKeepJobs = s.removeOnFail
	}
	keepJobs := s.getKeepJobs(shouldRemove, workerKeepJobs)

	opts := map[string]any{
		"token":          token,
		"keepJobs":       keepJobs,
		"lockDuration":   lockDurationMs,
		"attempts":       job.opts.Attempts,
		"attemptsMade":   job.attemptsMade,
		"maxMetricsSize": maxMetricsSize,
		"fpof":           job.opts.FailParentOnFailure,
		"rdof":           job.opts.RemoveDependencyOnFailure,
	}
	if s.limiter != nil {
		opts["limiter"] = map[string]any{
			"max":      s.limiter.Max,
			"duration": s.limiter.Duration,
		}
	}
	packedOpts, err := msgpack.Marshal(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal options: %w", err)
	}

	args := []any{
		job.id,
		timestamp.UnixMilli(),
		propValue,
		value,
		target,
		string(eventData),
		fetchNext,
		s.keyPrefix,
		packedOpts,
	}

	return keys, args, nil
}

// retryJobArgs builds the keys and args for the retryJob Lua script call.
func (s *scripts) retryJobArgs(jobId string, lifo bool, token string) ([]string, []any) {
	keys := []string{
		s.keyPrefix + "active",
		s.keyPrefix + "wait",
		s.keyPrefix + "paused",
		s.keyPrefix + jobId,
		s.keyPrefix + "meta",
		s.keyPrefix + "events",
		s.keyPrefix + "delayed",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "pc",
	}

	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}

	args := []any{
		s.keyPrefix,
		time.Now().UnixMilli(),
		pushCmd,
		jobId,
		token,
	}

	return keys, args
}

// moveToDelayedArgs builds the keys and args for the moveToDelayed Lua script call.
// timestampMillis represents when the job should be retried (in ms).
func (s *scripts) moveToDelayedArgs(jobId string, timestampMillis int64, token string) ([]string, []any) {
	// Normalize timestamp and bake in job id lower 12 bits like BullMQ
	if timestampMillis < 0 {
		timestampMillis = 0
	}
	var jobIdNumeric int64
	if parsed, err := strconv.ParseInt(jobId, 10, 64); err == nil {
		jobIdNumeric = parsed
	} else {
		jobIdNumeric = 0
	}
	var score int64
	if timestampMillis > 0 {
		score = timestampMillis*0x1000 + (jobIdNumeric & 0xfff)
	} else {
		score = 0
	}

	keys := []string{
		s.keyPrefix + "wait",
		s.keyPrefix + "active",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "delayed",
		s.keyPrefix + jobId,
		s.keyPrefix + "events",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
	}

	args := []any{
		s.keyPrefix,
		time.Now().UnixMilli(),
		fmt.Sprintf("%d", score),
		jobId,
		token,
	}

	return keys, args
}

// updateProgress updates the progress of a job.
func (s *scripts) updateProgress(ctx context.Context, jobId string, progress any) error {
	keys := []string{
		s.keyPrefix + jobId,
		s.keyPrefix + "events",
	}

	progressJson, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	result, err := lua.UpdateProgress(ctx, s.redisClient, keys, jobId, progressJson)
	if err != nil {
		return err
	}

	resultInt64, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type: %T", result)
	}

	if resultInt64 == -1 {
		return ErrJobNotFound
	}

	return nil
}

// updateData updates the job's data field atomically in Redis.
func (s *scripts) updateData(ctx context.Context, jobId string, data any) error {
	keys := []string{
		s.keyPrefix + jobId,
	}

	dataJson, err := json.Marshal(data)
	if err != nil {
		return err
	}

	result, err := lua.UpdateData(ctx, s.redisClient, keys, string(dataJson))
	if err != nil {
		return err
	}
	resultInt64, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type: %T", result)
	}
	if resultInt64 == -1 {
		return ErrJobNotFound
	}
	return nil
}

// moveJobFromActiveToWait moves a job back from Active to Wait when manually rate limited.
// It returns the remaining TTL (in ms) for the limiter key, clamped to 0 if negative.
func (s *scripts) moveJobFromActiveToWait(ctx context.Context, jobId string, token string) (int64, error) {
	keys := []string{
		s.keyPrefix + "active",
		s.keyPrefix + "wait",
		s.keyPrefix + "stalled",
		s.keyPrefix + jobId + ":lock",
		s.keyPrefix + "paused",
		s.keyPrefix + "meta",
		s.keyPrefix + "limiter",
		s.keyPrefix + "prioritized",
		s.keyPrefix + "events",
	}

	args := []any{
		jobId,
		token,
		s.keyPrefix + jobId,
	}

	result, err := lua.MoveJobFromActiveToWait(ctx, s.redisClient, keys, args...)
	if err != nil {
		return 0, err
	}

	pttl, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid result type from MoveJobFromActiveToWait: %T", result)
	}

	if pttl < 0 {
		return 0, nil
	}
	return pttl, nil
}
