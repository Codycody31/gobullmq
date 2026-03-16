package gobullmq

import (
	"context"
	"time"
)

// GetState returns the current state of the job.
func (j *Job[D]) GetState(ctx context.Context) (string, error) {
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

// ChangePriority changes the priority of the job.
func (j *Job[D]) ChangePriority(ctx context.Context, priority int, lifo bool) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobChangePriority(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, priority, lifo)
}

// ChangeDelay changes the delay of a delayed job.
func (j *Job[D]) ChangeDelay(ctx context.Context, delay time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobChangeDelay(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, delay.Milliseconds())
}

// Retry retries the job by moving it back to wait.
func (j *Job[D]) Retry(ctx context.Context, state string, lifo bool) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobRetry(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, state, lifo)
}

// UpdateData updates the job's data field in Redis.
func (j *Job[D]) UpdateData(ctx context.Context, data interface{}) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobUpdateData(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, data)
}

// UpdateProgress updates the progress of the job.
func (j *Job[D]) UpdateProgress(ctx context.Context, progress interface{}) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobUpdateProgress(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, progress)
}

// Log adds a log entry for the job.
func (j *Job[D]) Log(ctx context.Context, message string) (int64, error) {
	if !j.raw.hasJobContext() {
		return 0, ErrJobNoContext
	}
	return jobLog(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, message)
}

// GetLogs retrieves logs for the job with pagination.
func (j *Job[D]) GetLogs(ctx context.Context, start, end int64) ([]string, int64, error) {
	if !j.raw.hasJobContext() {
		return nil, 0, ErrJobNoContext
	}
	return jobGetLogs(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, start, end)
}

// ClearLogs clears all logs for the job.
func (j *Job[D]) ClearLogs(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobClearLogs(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// Remove removes the job from the queue.
func (j *Job[D]) Remove(ctx context.Context, removeChildren bool) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobRemove(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, removeChildren)
}

// ExtendLock extends the lock for the job.
func (j *Job[D]) ExtendLock(ctx context.Context, duration time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobExtendLock(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, j.raw.token, duration.Milliseconds())
}

// Discard marks the job to be discarded (not retried).
func (j *Job[D]) Discard(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobDiscard(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id)
}

// MoveToDelayed moves the job to the delayed set.
func (j *Job[D]) MoveToDelayed(ctx context.Context, delay time.Duration) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobMoveToDelayed(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, delay.Milliseconds(), j.raw.token)
}

// MoveToWaitingChildren moves the job to the waiting-children state.
func (j *Job[D]) MoveToWaitingChildren(ctx context.Context) error {
	if !j.raw.hasJobContext() {
		return ErrJobNoContext
	}
	return jobMoveToWaitingChildren(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, j.raw.token)
}

// WaitUntilFinished waits for the job to complete or fail using QueueEvents.
func (j *Job[D]) WaitUntilFinished(ctx context.Context, qe *QueueEvents, ttl time.Duration) (interface{}, error) {
	if !j.raw.hasJobContext() {
		return nil, ErrJobNoContext
	}
	return jobWaitUntilFinished(ctx, j.raw.client, j.raw.keyPrefix, j.raw.id, qe, ttl)
}

// GetChildrenValues returns the processed children values for a parent job.
func (j *Job[D]) GetChildrenValues(ctx context.Context) (map[string]interface{}, error) {
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
