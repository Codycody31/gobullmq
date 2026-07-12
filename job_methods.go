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

func jobGetState(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (JobState, error) {
	keys := []string{
		prefix + "completed",
		prefix + "failed",
		prefix + "delayed",
		prefix + "active",
		prefix + "wait",
		prefix + "paused",
		prefix + "waiting-children",
		prefix + "prioritized",
	}
	result, err := lua.GetState(ctx, client, keys, jobId)
	if err != nil {
		return "", fmt.Errorf("failed to get job state: %w", err)
	}
	state, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("unexpected result type from GetState: %T", result)
	}
	return JobState(state), nil
}

func jobPromote(ctx context.Context, client redis.Cmdable, prefix string, jobId string) error {
	keys := []string{
		prefix + "delayed",
		prefix + "wait",
		prefix + "paused",
		prefix + "meta",
		prefix + "prioritized",
		prefix + "pc",
		prefix + "events",
	}
	result, err := lua.Promote(ctx, client, keys, prefix, jobId)
	if err != nil {
		return fmt.Errorf("failed to promote job: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from promote: %T", result)
	}
	if code == -3 {
		return ErrJobNotInState
	}
	return nil
}

func jobChangePriority(ctx context.Context, client redis.Cmdable, prefix string, jobId string, priority int, lifo bool) error {
	keys := []string{
		prefix + "wait",
		prefix + "paused",
		prefix + "meta",
		prefix + "prioritized",
		prefix + "pc",
	}
	lifoStr := "0"
	if lifo {
		lifoStr = "1"
	}
	result, err := lua.ChangePriority(ctx, client, keys, priority, prefix+jobId, jobId, lifoStr)
	if err != nil {
		return fmt.Errorf("failed to change priority: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from changePriority: %T", result)
	}
	if code == -1 {
		return ErrJobNotFound
	}
	return nil
}

func jobChangeDelay(ctx context.Context, client redis.Cmdable, prefix string, jobId string, delayMs int64) error {
	timestamp := time.Now().UnixMilli() + delayMs
	var jobIdNumeric int64
	if parsed, err := strconv.ParseInt(jobId, 10, 64); err == nil {
		jobIdNumeric = parsed
	}
	var score int64
	if timestamp > 0 {
		score = timestamp*0x1000 + (jobIdNumeric & 0xfff)
	}

	keys := []string{
		prefix + "delayed",
		prefix + jobId,
		prefix + "events",
	}
	result, err := lua.ChangeDelay(ctx, client, keys, delayMs, fmt.Sprintf("%d", score), jobId)
	if err != nil {
		return fmt.Errorf("failed to change delay: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from changeDelay: %T", result)
	}
	switch code {
	case -1:
		return ErrJobNotFound
	case -3:
		return ErrJobNotInState
	}
	return nil
}

func jobRetry(ctx context.Context, client redis.Cmdable, prefix string, jobId string, state JobState, lifo bool) error {
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}
	propVal := "failedReason"
	if state == JobStateCompleted {
		propVal = "returnvalue"
	}
	keys := []string{
		prefix + jobId,
		prefix + "events",
		prefix + state.redisKey(),
		prefix + "wait",
		prefix + "meta",
		prefix + "paused",
	}
	result, err := lua.ReprocessJob(ctx, client, keys, jobId, pushCmd, propVal, state.redisKey())
	if err != nil {
		return fmt.Errorf("failed to retry job: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from retryJob: %T", result)
	}
	switch code {
	case -1:
		return ErrJobNotFound
	case -3:
		return ErrJobNotInState
	}
	return nil
}

func jobUpdateData(ctx context.Context, client redis.Cmdable, prefix string, jobId string, data any) error {
	s := newScripts(client, prefix)
	return s.updateData(ctx, jobId, data)
}

func jobUpdateProgress(ctx context.Context, client redis.Cmdable, prefix string, jobId string, progress any) error {
	s := newScripts(client, prefix)
	return s.updateProgress(ctx, jobId, progress)
}

// jobLog appends a log entry, trimming to keepLogs when set, mirroring
// upstream Job.addJobLog (RPUSH then LTRIM -keepLogs -1).
func jobLog(ctx context.Context, client redis.Cmdable, prefix string, jobId string, message string, keepLogs int64) (int64, error) {
	logsKey := prefix + jobId + ":logs"
	pipe := client.Pipeline()
	countCmd := pipe.RPush(ctx, logsKey, message)
	if keepLogs > 0 {
		pipe.LTrim(ctx, logsKey, -keepLogs, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("failed to add job log: %w", err)
	}
	count := countCmd.Val()
	if keepLogs > 0 && count > keepLogs {
		count = keepLogs
	}
	return count, nil
}

func jobClearLogs(ctx context.Context, client redis.Cmdable, prefix string, jobId string, keepLogs int64) error {
	logsKey := prefix + jobId + ":logs"
	if keepLogs > 0 {
		return client.LTrim(ctx, logsKey, -keepLogs, -1).Err()
	}
	return client.Del(ctx, logsKey).Err()
}

func jobIsWaitingChildren(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateWaitingChildren, err
}

// moveToFinishedCode extracts the status code from a moveToFinished result,
// which is either a bare code or an array whose first element is the code
// (or job data when fetchNext returned a job).
func moveToFinishedCode(result any) int64 {
	switch v := result.(type) {
	case int64:
		return v
	case []any:
		if len(v) > 0 {
			if n, ok := v[0].(int64); ok {
				return n
			}
		}
	}
	return 0
}

// jobGetLogs returns a page of the job's log list and its full length.
// SortDesc counts the page from the newest end and returns it newest first,
// mirroring upstream getJobLogs asc=false (LRANGE -(end+1) -(start+1), then
// reverse).
func jobGetLogs(ctx context.Context, client redis.Cmdable, prefix string, jobId string, start, end int64, order SortOrder) ([]string, int64, error) {
	logsKey := prefix + jobId + ":logs"
	pipe := client.Pipeline()
	var logsCmd *redis.StringSliceCmd
	if order == SortDesc {
		logsCmd = pipe.LRange(ctx, logsKey, -(end + 1), -(start + 1))
	} else {
		logsCmd = pipe.LRange(ctx, logsKey, start, end)
	}
	countCmd := pipe.LLen(ctx, logsKey)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get job logs: %w", err)
	}
	logs := logsCmd.Val()
	if order == SortDesc {
		for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
			logs[i], logs[j] = logs[j], logs[i]
		}
	}
	return logs, countCmd.Val(), nil
}

func jobIsCompleted(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateCompleted, err
}

func jobIsFailed(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateFailed, err
}

func jobIsDelayed(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateDelayed, err
}

func jobIsActive(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateActive, err
}

func jobIsWaiting(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == JobStateWaiting, err
}

func jobGetChildrenValues(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (map[string]any, error) {
	processedKey := prefix + jobId + ":processed"
	data, err := client.HGetAll(ctx, processedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get children values: %w", err)
	}
	result := make(map[string]any, len(data))
	for k, v := range data {
		result[k] = parseReturnValue(v)
	}
	return result, nil
}

// parseReturnValue mirrors upstream getReturnValue: the returnvalue field is
// stored as JSON; parse it, falling back to the raw string when the JSON is
// invalid.
func parseReturnValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return s
	}
	return parsed
}

func jobWaitUntilFinished(ctx context.Context, client redis.Cmdable, prefix string, jobId string, queueEvents *QueueEvents) (any, error) {
	if queueEvents == nil {
		return nil, fmt.Errorf("queueEvents must not be nil")
	}

	done := make(chan any, 1)
	errCh := make(chan error, 1)

	completedEvent := fmt.Sprintf("completed:%s", jobId)
	failedEvent := fmt.Sprintf("failed:%s", jobId)

	completedHandler := func(args ...any) {
		if len(args) >= 1 {
			if data, ok := args[0].(map[string]any); ok {
				select {
				case done <- data["returnvalue"]:
				default:
				}
				return
			}
		}
		select {
		case done <- nil:
		default:
		}
	}
	failedHandler := func(args ...any) {
		failErr := fmt.Errorf("job failed")
		if len(args) >= 1 {
			if data, ok := args[0].(map[string]any); ok {
				reason, _ := data["failedReason"].(string)
				failErr = fmt.Errorf("job failed: %s", reason)
			}
		}
		select {
		case errCh <- failErr:
		default:
		}
	}

	// Subscribe before checking current state, so a job finishing between the
	// check and the subscription cannot be missed (upstream registers
	// listeners first for the same reason).
	completedID := queueEvents.On(completedEvent, completedHandler)
	failedID := queueEvents.On(failedEvent, failedHandler)
	defer queueEvents.Off(completedEvent, completedID)
	defer queueEvents.Off(failedEvent, failedID)

	keys := []string{
		prefix + "completed",
		prefix + "failed",
		prefix + jobId,
	}
	result, err := lua.IsFinished(ctx, client, keys, jobId, "1")
	if err != nil {
		return nil, fmt.Errorf("failed to check if job is finished: %w", err)
	}
	if resultSlice, ok := result.([]any); ok && len(resultSlice) >= 2 {
		status, _ := resultSlice[0].(int64)
		value := resultSlice[1]
		switch status {
		case 1:
			return parseReturnValue(value), nil
		case 2:
			return nil, fmt.Errorf("job failed: %v", value)
		case -1:
			return nil, ErrJobNotFound
		}
	}

	select {
	case val := <-done:
		return parseReturnValue(val), nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func jobRemove(ctx context.Context, client redis.Cmdable, prefix string, jobId string, removeChildren bool) error {
	keys := []string{prefix}
	result, err := lua.RemoveJob(ctx, client, keys, jobId, removeChildren)
	if err != nil {
		return fmt.Errorf("failed to remove job: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from removeJob: %T", result)
	}
	if code == 0 {
		return ErrJobLocked
	}
	return nil
}

func jobExtendLock(ctx context.Context, client redis.Cmdable, prefix string, jobId string, token any, durationMs int64) error {
	keys := []string{
		prefix + jobId + ":lock",
		prefix + "stalled",
	}
	result, err := lua.ExtendLock(ctx, client, keys, token, durationMs, jobId)
	if err != nil {
		return err
	}
	if n, ok := result.(int64); ok && n == 0 {
		return ErrMissingLock
	}
	return nil
}

func jobMoveToDelayed(ctx context.Context, client redis.Cmdable, prefix string, jobId string, delayMs int64, token string) error {
	timestamp := time.Now().UnixMilli() + delayMs
	s := newScripts(client, prefix)
	keys, args := s.moveToDelayedArgs(jobId, timestamp, token)
	_, err := lua.MoveToDelayed(ctx, client, keys, args...)
	if err != nil {
		return fmt.Errorf("failed to move job to delayed: %w", err)
	}
	return nil
}

// jobMoveToWaitingChildren returns true when the job was moved to
// waiting-children (code 0) and false when it was not (code 1), mirroring
// upstream Scripts.moveToWaitingChildren's boolean.
func jobMoveToWaitingChildren(ctx context.Context, client redis.Cmdable, prefix string, jobId string, token string) (bool, error) {
	keys := []string{
		prefix + jobId + ":lock",
		prefix + "active",
		prefix + "waiting-children",
		prefix + jobId,
	}
	result, err := lua.MoveToWaitingChildren(ctx, client, keys, token, "", time.Now().UnixMilli(), jobId)
	if err != nil {
		return false, fmt.Errorf("failed to move to waiting-children: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected result type from moveToWaitingChildren: %T", result)
	}
	switch code {
	case 0:
		return true, nil
	case 1:
		return false, nil
	case -1:
		return false, ErrJobNotFound
	case -2:
		return false, ErrMissingLock
	case -3:
		return false, ErrJobNotInState
	default:
		return false, fmt.Errorf("bullmq: unexpected code %d from moveToWaitingChildren for job %s", code, jobId)
	}
}
