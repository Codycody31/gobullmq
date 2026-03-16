package gobullmq

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
)

func jobGetState(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (string, error) {
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
	return state, nil
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

func jobRetry(ctx context.Context, client redis.Cmdable, prefix string, jobId string, state string, lifo bool) error {
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}
	propVal := "failedReason"
	if state == "completed" {
		propVal = "returnvalue"
	}
	keys := []string{
		prefix + jobId,
		prefix + "events",
		prefix + state,
		prefix + "wait",
		prefix + "meta",
		prefix + "paused",
	}
	result, err := lua.ReprocessJob(ctx, client, keys, jobId, pushCmd, propVal, state)
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

func jobUpdateData(ctx context.Context, client redis.Cmdable, prefix string, jobId string, data interface{}) error {
	s := newScripts(client, prefix)
	return s.updateData(ctx, jobId, data)
}

func jobUpdateProgress(ctx context.Context, client redis.Cmdable, prefix string, jobId string, progress interface{}) error {
	s := newScripts(client, prefix)
	return s.updateProgress(ctx, jobId, progress)
}

func jobLog(ctx context.Context, client redis.Cmdable, prefix string, jobId string, message string) (int64, error) {
	logsKey := prefix + jobId + ":logs"
	count, err := client.RPush(ctx, logsKey, message).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to add job log: %w", err)
	}
	return count, nil
}

func jobClearLogs(ctx context.Context, client redis.Cmdable, prefix string, jobId string) error {
	logsKey := prefix + jobId + ":logs"
	_, err := client.Del(ctx, logsKey).Result()
	return err
}

func jobGetLogs(ctx context.Context, client redis.Cmdable, prefix string, jobId string, start, end int64) ([]string, int64, error) {
	logsKey := prefix + jobId + ":logs"
	pipe := client.Pipeline()
	logsCmd := pipe.LRange(ctx, logsKey, start, end)
	countCmd := pipe.LLen(ctx, logsKey)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get job logs: %w", err)
	}
	return logsCmd.Val(), countCmd.Val(), nil
}

func jobIsCompleted(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == "completed", err
}

func jobIsFailed(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == "failed", err
}

func jobIsDelayed(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == "delayed", err
}

func jobIsActive(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == "active", err
}

func jobIsWaiting(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (bool, error) {
	state, err := jobGetState(ctx, client, prefix, jobId)
	return state == string(JobStateWaiting), err
}

func jobGetChildrenValues(ctx context.Context, client redis.Cmdable, prefix string, jobId string) (map[string]interface{}, error) {
	processedKey := prefix + jobId + ":processed"
	data, err := client.HGetAll(ctx, processedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get children values: %w", err)
	}
	result := make(map[string]interface{}, len(data))
	for k, v := range data {
		result[k] = v
	}
	return result, nil
}

func jobWaitUntilFinished(ctx context.Context, client redis.Cmdable, prefix string, jobId string, queueEvents *QueueEvents, ttl time.Duration) (interface{}, error) {
	if queueEvents == nil {
		return nil, fmt.Errorf("queueEvents must not be nil")
	}
	keys := []string{
		prefix + "completed",
		prefix + "failed",
		prefix + jobId,
	}
	result, err := lua.IsFinished(ctx, client, keys, jobId, "1")
	if err != nil {
		return nil, fmt.Errorf("failed to check if job is finished: %w", err)
	}
	if resultSlice, ok := result.([]interface{}); ok && len(resultSlice) >= 2 {
		status, _ := resultSlice[0].(int64)
		value := resultSlice[1]
		switch status {
		case 1:
			return value, nil
		case 2:
			return nil, fmt.Errorf("job failed: %v", value)
		case -1:
			return nil, ErrJobNotFound
		}
	}

	done := make(chan interface{}, 1)
	errCh := make(chan error, 1)

	completedEvent := fmt.Sprintf("completed:%s", jobId)
	failedEvent := fmt.Sprintf("failed:%s", jobId)

	completedHandler := func(args ...interface{}) {
		if len(args) >= 1 {
			if data, ok := args[0].(map[string]interface{}); ok {
				done <- data["returnvalue"]
			} else {
				done <- nil
			}
		} else {
			done <- nil
		}
	}
	failedHandler := func(args ...interface{}) {
		if len(args) >= 1 {
			if data, ok := args[0].(map[string]interface{}); ok {
				reason, _ := data["failedReason"].(string)
				errCh <- fmt.Errorf("job failed: %s", reason)
			} else {
				errCh <- fmt.Errorf("job failed")
			}
		} else {
			errCh <- fmt.Errorf("job failed")
		}
	}

	completedID := queueEvents.On(completedEvent, completedHandler)
	failedID := queueEvents.On(failedEvent, failedHandler)
	defer queueEvents.Off(completedEvent, completedID)
	defer queueEvents.Off(failedEvent, failedID)

	if ttl > 0 {
		timer := time.NewTimer(ttl)
		defer timer.Stop()
		select {
		case val := <-done:
			return val, nil
		case err := <-errCh:
			return nil, err
		case <-timer.C:
			return nil, fmt.Errorf("timeout waiting for job %s after %v", jobId, ttl)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	select {
	case val := <-done:
		return val, nil
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

func jobExtendLock(ctx context.Context, client redis.Cmdable, prefix string, jobId string, token interface{}, durationMs int64) error {
	keys := []string{
		prefix + "lock",
		prefix + "stalled",
	}
	_, err := lua.ExtendLock(ctx, client, keys, token, durationMs, jobId)
	return err
}

func jobDiscard(ctx context.Context, client redis.Cmdable, prefix string, jobId string) error {
	jobKey := prefix + jobId
	_, err := client.HSet(ctx, jobKey, "discarded", "1").Result()
	return err
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

func jobMoveToWaitingChildren(ctx context.Context, client redis.Cmdable, prefix string, jobId string, token string) error {
	keys := []string{
		prefix + jobId + ":lock",
		prefix + "active",
		prefix + "waiting-children",
		prefix + jobId,
	}
	result, err := lua.MoveToWaitingChildren(ctx, client, keys, token, "", time.Now().UnixMilli(), jobId)
	if err != nil {
		return fmt.Errorf("failed to move to waiting-children: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from moveToWaitingChildren: %T", result)
	}
	switch code {
	case -1:
		return ErrJobNotFound
	case -2:
		return ErrMissingLock
	case -3:
		return ErrJobNotInState
	}
	return nil
}
