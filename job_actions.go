package gobullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.codycody31.dev/gobullmq/internal/lua"
	backoffutil "go.codycody31.dev/gobullmq/internal/utils/backoff"
)

// State returns the current state of the job.
func (j *Job[D]) State(ctx context.Context) (JobState, error) {
	if !j.raw.hasJobContext() {
		return "", ErrJobNoContext
	}
	return jobGetState(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// Promote promotes a delayed job to the waiting state.
func (j *Job[D]) Promote(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobPromote(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// ChangePriority changes the priority of the job. placement selects where
// the job re-enters the wait list when it loses its priority (FIFO or LIFO).
func (j *Job[D]) ChangePriority(ctx context.Context, priority int, placement Placement) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobChangePriority(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, priority, placement == LIFO)
}

// ChangeDelay changes the delay of a delayed job.
func (j *Job[D]) ChangeDelay(ctx context.Context, delay time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobChangeDelay(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, delay.Milliseconds())
}

// Retry retries the job by moving it back to wait. state names the finished
// set the job currently sits in and must be JobStateFailed or
// JobStateCompleted; placement selects where the job re-enters the wait list.
func (j *Job[D]) Retry(ctx context.Context, state JobState, placement Placement) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	if state != JobStateFailed && state != JobStateCompleted {
		return fmt.Errorf("bullmq: retry state must be %q or %q, got %q", JobStateFailed, JobStateCompleted, state)
	}
	return jobRetry(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, state, placement == LIFO)
}

// UpdateData updates the job's data field in Redis and, on success, the
// in-memory Data of this instance.
func (j *Job[D]) UpdateData(ctx context.Context, data D) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	if err := jobUpdateData(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, data); err != nil {
		return err
	}
	j.data = data
	return nil
}

// UpdateProgress updates the progress of the job.
func (j *Job[D]) UpdateProgress(ctx context.Context, progress any) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobUpdateProgress(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, progress)
}

// Log adds a log entry for the job, honoring the job's KeepLogs option.
func (j *Job[D]) Log(ctx context.Context, message string) (int64, error) {
	if !j.raw.hasJobContext() {
		return 0, ErrJobNoContext
	}
	return jobLog(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, message, int64(j.raw.opts.KeepLogs))
}

// Logs retrieves logs for the job with pagination, oldest first.
func (j *Job[D]) Logs(ctx context.Context, start, end int64) ([]string, int64, error) {
	if !j.raw.hasJobContext() {
		return nil, 0, ErrJobNoContext
	}
	return jobGetLogs(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, start, end, SortAsc)
}

// ClearLogs clears logs for the job, retaining only the keep most recent
// entries (0 clears all), mirroring upstream clearLogs(keepLogs).
func (j *Job[D]) ClearLogs(ctx context.Context, keep int64) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobClearLogs(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, keep)
}

// Remove removes the job from the queue. Child jobs are kept; use
// RemoveWithChildren to remove them too.
func (j *Job[D]) Remove(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobRemove(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, false)
}

// RemoveWithChildren removes the job and its children from the queue.
func (j *Job[D]) RemoveWithChildren(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobRemove(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, true)
}

// ExtendLock extends the lock for the job.
func (j *Job[D]) ExtendLock(ctx context.Context, duration time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobExtendLock(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, j.raw.token, duration.Milliseconds())
}

// Discard marks the job as not to be retried: on failure it moves straight to
// the failed set regardless of remaining attempts. Like upstream Job.discard()
// this is an in-memory flag, effective for the worker currently processing
// the job.
func (j *Job[D]) Discard() {
	j.raw.discarded = true
}

// MoveToDelayed moves the job to the delayed set.
func (j *Job[D]) MoveToDelayed(ctx context.Context, delay time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobMoveToDelayed(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, delay.Milliseconds(), j.raw.token)
}

// MoveToWaitingChildren moves the job to the waiting-children state. It
// returns true when the job was moved and false when it was not, mirroring
// upstream job.moveToWaitingChildren's boolean.
func (j *Job[D]) MoveToWaitingChildren(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobMoveToWaitingChildren(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, j.raw.token)
}

// WaitUntilFinished waits for the job to complete or fail using QueueEvents,
// returning the JSON-decoded return value (or the raw string when it is not
// valid JSON, like upstream getReturnValue). Bound the wait with
// context.WithTimeout.
func (j *Job[D]) WaitUntilFinished(ctx context.Context, qe *QueueEvents) (any, error) {
	if !j.raw.hasJobContext() {
		return nil, ErrJobNoContext
	}
	return jobWaitUntilFinished(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, qe)
}

// ChildrenValues returns the processed children values for a parent job.
func (j *Job[D]) ChildrenValues(ctx context.Context) (map[string]any, error) {
	if !j.raw.hasJobContext() {
		return nil, ErrJobNoContext
	}
	return jobGetChildrenValues(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsCompleted checks if the job is in the completed state.
func (j *Job[D]) IsCompleted(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsCompleted(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsFailed checks if the job is in the failed state.
func (j *Job[D]) IsFailed(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsFailed(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsDelayed checks if the job is in the delayed state.
func (j *Job[D]) IsDelayed(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsDelayed(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsActive checks if the job is in the active state.
func (j *Job[D]) IsActive(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsActive(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsWaiting checks if the job is in the waiting state.
func (j *Job[D]) IsWaiting(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsWaiting(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// IsWaitingChildren checks if the job is in the waiting-children state.
func (j *Job[D]) IsWaitingChildren(ctx context.Context) (bool, error) {
	if !j.raw.hasJobContext() {
		return false, ErrJobNoContext
	}
	return jobIsWaitingChildren(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// DependenciesCount returns the number of processed and unprocessed child
// dependencies for a parent job.
func (j *Job[D]) DependenciesCount(ctx context.Context) (processed int64, unprocessed int64, err error) {
	if !j.raw.hasJobContext() {
		return 0, 0, ErrJobNoContext
	}
	jobKey := j.raw.keyPrefix + j.raw.id
	pipe := j.raw.client.Pipeline()
	processedCmd := pipe.HLen(ctx, jobKey+":processed")
	unprocessedCmd := pipe.SCard(ctx, jobKey+":dependencies")
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, 0, err
	}
	return processedCmd.Val(), unprocessedCmd.Val(), nil
}

// MoveToCompleted moves the job to the completed set with the given return
// value, using the token the job was fetched with (manual processing). When
// fetchNext is true and another job was waiting, that job is returned already
// locked with the same token, mirroring upstream job.moveToCompleted.
func (j *Job[D]) MoveToCompleted(ctx context.Context, returnValue any, fetchNext bool) (*Job[D], error) {
	if !j.raw.hasJobContext() {
		return nil, ErrJobNoContext
	}
	val, err := json.Marshal(returnValue)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal return value: %w", err)
	}
	s := newScripts(j.raw.client, j.raw.keyPrefix)
	keys, args, err := s.moveToFinishedArgs(j.raw, string(val), "returnvalue", j.raw.opts.RemoveOnComplete, "completed", j.raw.token, time.Now(), fetchNext, int(defaultLockDuration/time.Millisecond), "")
	if err != nil {
		return nil, err
	}
	result, err := lua.MoveToFinished(ctx, j.raw.client, keys, args...)
	if err != nil {
		return nil, err
	}
	if code := moveToFinishedCode(result); code < 0 {
		return nil, finishedErrors(code, j.raw.id, "moveToCompleted")
	}
	j.raw.returnValue = returnValue

	// The prefetched next job (if any) is locked with our token; hand it to
	// the caller instead of stranding it in active.
	if arr, ok := result.([]any); ok {
		if next := raw2NextJobData(arr); next != nil && next.JobData != nil && next.ID != "" && next.ID != "0" {
			nextRaw, err := jobFromJson(next.JobData)
			if err != nil {
				return nil, fmt.Errorf("failed to parse fetched next job: %w", err)
			}
			nextRaw.id = next.ID
			nextRaw.token = j.raw.token
			nextRaw.setJobContext(j.raw.client, j.raw.keyPrefix)
			return wrapRawJob[D](&nextRaw)
		}
	}
	return nil, nil
}

// MoveToFailed moves the job to the failed set (or schedules a retry when
// attempts remain, honoring the job's backoff, discard flag, and
// ErrUnrecoverable), using the token the job was fetched with. The fetchNext
// argument is retained for API compatibility but disabled because this
// error-only method cannot return ownership of a prefetched job.
func (j *Job[D]) MoveToFailed(ctx context.Context, jobErr error, _ bool) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	raw := j.raw
	s := newScripts(raw.client, raw.keyPrefix)

	// failedReason and the stacktrace are persisted atomically with the
	// state move, like upstream's single multi in job.moveToFailed.
	stacktrace, err := raw.prepareStacktrace(jobErr)
	if err != nil {
		return err
	}
	jobKey := raw.keyPrefix + raw.id

	if raw.attemptsMade < raw.opts.Attempts && !raw.discarded && !errors.Is(jobErr, ErrUnrecoverable) {
		delayMs := 0
		if raw.opts.Backoff != nil {
			d, err := backoffutil.Calculate(backoffutil.Options{Type: raw.opts.Backoff.Type, Delay: raw.opts.Backoff.Delay}, raw.attemptsMade)
			if err != nil {
				return err
			}
			delayMs = d
		}
		if delayMs >= 0 {
			if delayMs > 0 {
				keys, args := s.moveToDelayedArgs(raw.id, time.Now().UnixMilli()+int64(delayMs), raw.token)
				result, err := runFailureMulti(ctx, raw.client, jobKey, stacktrace, raw.failedReason, lua.MoveToDelayedScript, keys, args)
				if err != nil {
					return fmt.Errorf("failed to move job %s to delayed: %w", raw.id, err)
				}
				if code, ok := result.(int64); ok && code < 0 {
					return finishedErrors(code, raw.id, "moveToDelayed")
				}
				return nil
			}
			keys, args := s.retryJobArgs(raw.id, raw.opts.Lifo, raw.token)
			result, err := runFailureMulti(ctx, raw.client, jobKey, stacktrace, raw.failedReason, lua.RetryJobScript, keys, args)
			if err != nil {
				return fmt.Errorf("failed to retry job %s: %w", raw.id, err)
			}
			if code, ok := result.(int64); ok && code < 0 {
				return finishedErrors(code, raw.id, "retryJob")
			}
			return nil
		}
		// delayMs == -1: the strategy vetoed the retry; fall through to failed.
	}

	// This error-only API cannot hand a prefetched job back to the caller.
	// Always disable fetch-next rather than moving a job to active and losing
	// its payload until stalled recovery.
	return jobMoveToFailed(ctx, s, raw, stacktrace, raw.token, false, int(defaultLockDuration/time.Millisecond), "")
}
