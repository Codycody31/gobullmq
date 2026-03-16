package gobullmq

import "errors"

// Sentinel errors for flow control.
var (
	ErrRateLimit       = errors.New("bullmq: rate limit exceeded")
	ErrDelayed         = errors.New("bullmq: delayed")
	ErrWaitingChildren = errors.New("bullmq: waiting children")
)

// Sentinel errors for job operations.
var (
	ErrJobNotFound   = errors.New("bullmq: job not found")
	ErrJobNoContext  = errors.New("bullmq: job has no queue context; retrieve via Queue.GetJob() or worker processing")
	ErrJobLocked     = errors.New("bullmq: job is locked")
	ErrJobNotInState = errors.New("bullmq: job not in expected state")
	ErrMissingLock   = errors.New("bullmq: missing lock for job")
)

// Sentinel errors for queue operations.
var (
	ErrQueueClosed    = errors.New("bullmq: queue is closed")
	ErrQueueNotPaused = errors.New("bullmq: cannot obliterate non-paused queue")
	ErrQueueHasActive = errors.New("bullmq: cannot obliterate queue with active jobs")
)

// Sentinel errors for worker operations.
var (
	ErrWorkerRunning   = errors.New("bullmq: worker is already running")
	ErrWorkerClosing   = errors.New("bullmq: worker is closing")
	ErrShutdownTimeout = errors.New("bullmq: worker shutdown timed out")
)
