package gobullmq

import "time"

// AddOption defines the functional option type for Queue.Add
type AddOption func(*JobOptions)

// AddWithPriority sets the priority for the job.
func AddWithPriority(priority int) AddOption {
	return func(o *JobOptions) {
		o.Priority = priority
	}
}

// AddWithRemoveOnComplete configures job removal upon successful completion.
// KeepJobs{Count: 0} removes the job immediately.
func AddWithRemoveOnComplete(keep KeepJobs) AddOption {
	return func(o *JobOptions) {
		setting := keep
		o.RemoveOnComplete = &setting
	}
}

// AddWithRemoveOnFail configures job removal upon failure.
// KeepJobs{Count: 0} removes the job immediately.
func AddWithRemoveOnFail(keep KeepJobs) AddOption {
	return func(o *JobOptions) {
		setting := keep
		o.RemoveOnFail = &setting
	}
}

// AddWithAttempts sets the maximum number of attempts for the job.
func AddWithAttempts(times int) AddOption {
	return func(o *JobOptions) {
		if times >= 0 {
			o.Attempts = times
		}
	}
}

// AddWithDelay sets an initial delay before the job can be processed.
func AddWithDelay(delay time.Duration) AddOption {
	return func(o *JobOptions) {
		if delay >= 0 {
			o.Delay = int(delay.Milliseconds())
		}
	}
}

// AddWithTimestamp sets a custom timestamp for the job.
func AddWithTimestamp(tsMillis int64) AddOption {
	return func(o *JobOptions) {
		o.Timestamp = tsMillis
	}
}

// AddWithJobID sets a specific ID for the job.
func AddWithJobID(id string) AddOption {
	return func(o *JobOptions) {
		o.JobID = id
	}
}

// AddWithRepeat configures the job to repeat based on the provided options.
func AddWithRepeat(repeatOpts JobRepeatOptions) AddOption {
	return func(o *JobOptions) {
		o.Repeat = &repeatOpts
	}
}

// AddWithLifo adds the job using LIFO order.
func AddWithLifo() AddOption {
	return func(o *JobOptions) {
		o.Lifo = true
	}
}

// AddWithFailParentOnFailure marks the job to fail its parent job if this job fails.
func AddWithFailParentOnFailure(fail bool) AddOption {
	return func(o *JobOptions) {
		o.FailParentOnFailure = fail
	}
}

// AddWithParent sets the parent job information for this job.
func AddWithParent(parentOpts ParentOptions) AddOption {
	return func(o *JobOptions) {
		o.Parent = &parentOpts
	}
}

// AddWithRemoveDependencyOnFailure marks the job's dependency to be removed from its parent even if this job fails.
func AddWithRemoveDependencyOnFailure(remove bool) AddOption {
	return func(o *JobOptions) {
		o.RemoveDependencyOnFailure = remove
	}
}

// AddWithBackoff sets per-job backoff strategy, overriding worker-level backoff.
func AddWithBackoff(opts BackoffOptions) AddOption {
	return func(o *JobOptions) {
		o.Backoff = &opts
	}
}

// AddWithKeepLogs caps the number of log entries kept for the job.
func AddWithKeepLogs(n int) AddOption {
	return func(o *JobOptions) {
		o.KeepLogs = n
	}
}

// AddWithStackTraceLimit caps the stacktrace entries kept on job failures.
func AddWithStackTraceLimit(n int) AddOption {
	return func(o *JobOptions) {
		o.StackTraceLimit = n
	}
}

// AddWithSizeLimit rejects the job at add time if its JSON data exceeds n bytes.
func AddWithSizeLimit(n int) AddOption {
	return func(o *JobOptions) {
		o.SizeLimit = n
	}
}
