package gobullmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
)

// allStates is the default set of states for GetJobCounts.
var allStates = []string{"wait", "active", "completed", "failed", "delayed", "paused", "prioritized", "waiting-children"}

// GetJob retrieves a job by its ID.
func (q *Queue[D]) GetJob(ctx context.Context, jobId string) (*Job[D], error) {
	raw, err := jobFromId(ctx, q.client, q.keyPrefix, jobId)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, nil
		}
		return nil, err
	}
	raw.setJobContext(q.client, q.keyPrefix)
	return wrapRawJob[D](&raw)
}

// GetJobCounts returns counts for the specified states.
func (q *Queue[D]) GetJobCounts(ctx context.Context, states ...string) (map[string]int64, error) {
	if len(states) == 0 {
		states = allStates
	}
	keys := []string{q.keyPrefix}
	args := make([]interface{}, len(states))
	for i, s := range states {
		args[i] = s
	}
	result, err := lua.GetCounts(ctx, q.client, keys, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get job counts: %w", err)
	}
	counts := make(map[string]int64, len(states))
	if resultSlice, ok := result.([]interface{}); ok {
		for i, s := range states {
			if i < len(resultSlice) {
				if v, ok := resultSlice[i].(int64); ok {
					counts[s] = v
				}
			}
		}
	}
	return counts, nil
}

// GetJobs returns jobs for the specified states with pagination.
func (q *Queue[D]) GetJobs(ctx context.Context, states []string, start, end int, asc bool) ([]*Job[D], error) {
	keys := []string{q.keyPrefix}
	ascStr := "0"
	if asc {
		ascStr = "1"
	}
	args := []interface{}{start, end, ascStr}
	for _, s := range states {
		args = append(args, s)
	}
	result, err := lua.GetRanges(ctx, q.client, keys, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs: %w", err)
	}

	var jobs []*Job[D]
	resultSlice, ok := result.([]interface{})
	if !ok {
		return jobs, nil
	}

	var errs []error
	for _, stateResult := range resultSlice {
		jobIds, ok := stateResult.([]interface{})
		if !ok {
			continue
		}
		for _, id := range jobIds {
			idStr := fmt.Sprintf("%v", id)
			if idStr == "" || strings.HasPrefix(idStr, "0:") {
				continue
			}
			raw, err := jobFromId(ctx, q.client, q.keyPrefix, idStr)
			if err != nil {
				if errors.Is(err, ErrJobNotFound) {
					continue
				}
				errs = append(errs, fmt.Errorf("failed to load job %s: %w", idStr, err))
				continue
			}
			raw.setJobContext(q.client, q.keyPrefix)
			typed, err := wrapRawJob[D](&raw)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to deserialize job %s: %w", idStr, err))
				continue
			}
			jobs = append(jobs, typed)
		}
	}
	if len(errs) > 0 {
		q.Emit("error", fmt.Sprintf("GetJobs encountered %d errors loading jobs", len(errs)))
		return jobs, errors.Join(errs...)
	}
	return jobs, nil
}

// GetJobState returns the state of a specific job.
func (q *Queue[D]) GetJobState(ctx context.Context, jobId string) (string, error) {
	return jobGetState(ctx, q.client, q.keyPrefix, jobId)
}

// --- State-specific getters ---

// GetActive returns jobs currently being processed.
func (q *Queue[D]) GetActive(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"active"}, start, end, true)
}

// GetCompleted returns jobs that have been successfully completed.
func (q *Queue[D]) GetCompleted(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"completed"}, start, end, false)
}

// GetDelayed returns jobs that are scheduled to run after a delay.
func (q *Queue[D]) GetDelayed(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"delayed"}, start, end, false)
}

// GetFailed returns jobs that have failed processing.
func (q *Queue[D]) GetFailed(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"failed"}, start, end, false)
}

// GetWaiting returns jobs waiting to be processed.
func (q *Queue[D]) GetWaiting(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"wait"}, start, end, true)
}

// GetPrioritized returns jobs ordered by priority.
func (q *Queue[D]) GetPrioritized(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"prioritized"}, start, end, false)
}

// GetWaitingChildren returns parent jobs waiting for their children to complete.
func (q *Queue[D]) GetWaitingChildren(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.GetJobs(ctx, []string{"waiting-children"}, start, end, false)
}

// --- State-specific counters ---

// GetActiveCount returns the number of jobs currently being processed.
func (q *Queue[D]) GetActiveCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "active")
	if err != nil {
		return 0, err
	}
	return counts["active"], nil
}

// GetCompletedCount returns the number of completed jobs.
func (q *Queue[D]) GetCompletedCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "completed")
	if err != nil {
		return 0, err
	}
	return counts["completed"], nil
}

// GetDelayedCount returns the number of delayed jobs.
func (q *Queue[D]) GetDelayedCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "delayed")
	if err != nil {
		return 0, err
	}
	return counts["delayed"], nil
}

// GetFailedCount returns the number of failed jobs.
func (q *Queue[D]) GetFailedCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "failed")
	if err != nil {
		return 0, err
	}
	return counts["failed"], nil
}

// GetWaitingCount returns the number of jobs waiting to be processed.
func (q *Queue[D]) GetWaitingCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "wait")
	if err != nil {
		return 0, err
	}
	return counts["wait"], nil
}

// GetPrioritizedCount returns the number of prioritized jobs.
func (q *Queue[D]) GetPrioritizedCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "prioritized")
	if err != nil {
		return 0, err
	}
	return counts["prioritized"], nil
}

// GetWaitingChildrenCount returns the number of parent jobs waiting for children.
func (q *Queue[D]) GetWaitingChildrenCount(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "waiting-children")
	if err != nil {
		return 0, err
	}
	return counts["waiting-children"], nil
}

// Count returns the total number of pending jobs (waiting + paused).
func (q *Queue[D]) Count(ctx context.Context) (int64, error) {
	counts, err := q.GetJobCounts(ctx, "wait", "paused")
	if err != nil {
		return 0, err
	}
	return counts["wait"] + counts["paused"], nil
}

// AddBulk adds multiple jobs to the queue.
func (q *Queue[D]) AddBulk(ctx context.Context, jobs []BulkJob[D]) ([]*Job[D], error) {
	results := make([]*Job[D], 0, len(jobs))
	for _, j := range jobs {
		addOpts := optsToAddOptions(j.Opts)
		job, err := q.Add(ctx, j.Name, j.Data, addOpts...)
		if err != nil {
			return results, fmt.Errorf("failed to add job %s in bulk: %w", j.Name, err)
		}
		results = append(results, job)
	}
	return results, nil
}

// optsToAddOptions converts JobOptions fields into AddOption functions.
func optsToAddOptions(opts JobOptions) []AddOption {
	var addOpts []AddOption
	if opts.Priority != 0 {
		addOpts = append(addOpts, AddWithPriority(opts.Priority))
	}
	if opts.Delay != 0 {
		addOpts = append(addOpts, AddWithDelay(time.Duration(opts.Delay)*time.Millisecond))
	}
	if opts.Attempts > 0 {
		addOpts = append(addOpts, AddWithAttempts(opts.Attempts))
	}
	if opts.RemoveOnComplete != nil {
		addOpts = append(addOpts, AddWithRemoveOnComplete(*opts.RemoveOnComplete))
	}
	if opts.RemoveOnFail != nil {
		addOpts = append(addOpts, AddWithRemoveOnFail(*opts.RemoveOnFail))
	}
	if opts.JobID != "" {
		addOpts = append(addOpts, AddWithJobID(opts.JobID))
	}
	if opts.Timestamp != 0 {
		addOpts = append(addOpts, AddWithTimestamp(opts.Timestamp))
	}
	if opts.Repeat != nil {
		addOpts = append(addOpts, AddWithRepeat(*opts.Repeat))
	}
	if opts.Lifo {
		addOpts = append(addOpts, AddWithLifo())
	}
	if opts.Parent != nil {
		addOpts = append(addOpts, AddWithParent(*opts.Parent))
	}
	if opts.FailParentOnFailure {
		addOpts = append(addOpts, AddWithFailParentOnFailure(true))
	}
	if opts.RemoveDependencyOnFailure {
		addOpts = append(addOpts, AddWithRemoveDependencyOnFailure(true))
	}
	if opts.Backoff != nil {
		addOpts = append(addOpts, AddWithBackoff(*opts.Backoff))
	}
	if opts.WaitChildren {
		addOpts = append(addOpts, func(o *JobOptions) { o.WaitChildren = true })
	}
	return addOpts
}

// RepeatableJobInfo holds metadata for a repeatable job entry.
type RepeatableJobInfo struct {
	Key     string
	Name    string
	ID      string
	EndDate string
	TZ      string
	Pattern string
	Every   string
	Next    int64
}

// GetRepeatableJobs returns repeatable job entries with pagination and sort order.
func (q *Queue[D]) GetRepeatableJobs(ctx context.Context, start, end int, asc bool) ([]RepeatableJobInfo, error) {
	key := q.toKey("repeat")
	var members []struct {
		Score  float64
		Member string
	}

	var zSlice []redis.Z
	var err error
	if asc {
		zSlice, err = q.client.ZRangeWithScores(ctx, key, int64(start), int64(end)).Result()
	} else {
		zSlice, err = q.client.ZRevRangeWithScores(ctx, key, int64(start), int64(end)).Result()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get repeatable jobs: %w", err)
	}
	for _, z := range zSlice {
		memberStr, ok := z.Member.(string)
		if !ok {
			continue
		}
		members = append(members, struct {
			Score  float64
			Member string
		}{Score: z.Score, Member: memberStr})
	}

	results := make([]RepeatableJobInfo, 0, len(members))
	for _, m := range members {
		info := parseRepeatKey(m.Member)
		info.Key = m.Member
		info.Next = int64(m.Score)
		results = append(results, info)
	}
	return results, nil
}

func parseRepeatKey(key string) RepeatableJobInfo {
	parts := strings.SplitN(key, ":", 5)
	info := RepeatableJobInfo{}
	if len(parts) >= 1 {
		info.Name = parts[0]
	}
	if len(parts) >= 2 {
		info.ID = parts[1]
	}
	if len(parts) >= 3 {
		info.EndDate = parts[2]
	}
	if len(parts) >= 4 {
		info.TZ = parts[3]
	}
	if len(parts) >= 5 {
		info.Pattern = parts[4]
		info.Every = parts[4]
	}
	return info
}

// RemoveRepeatable removes a repeatable job by name and repeat options.
func (q *Queue[D]) RemoveRepeatable(ctx context.Context, name string, repeatOpts JobRepeatOptions) error {
	repeatJobKey := repeat.GetKey(name, repeat.RepeatKeyOpts{
		EndDate: repeatOpts.EndDate,
		TZ:      repeatOpts.TZ,
		Pattern: repeatOpts.Pattern,
		Every:   repeatOpts.Every,
		JobId:   repeatOpts.JobID,
	})
	return q.removeRepeatableByKey(ctx, name, repeatJobKey)
}

// RemoveRepeatableByKey removes a repeatable job by its unique key.
func (q *Queue[D]) RemoveRepeatableByKey(ctx context.Context, key string) error {
	name := key
	if idx := strings.Index(key, ":"); idx >= 0 {
		name = key[:idx]
	}
	return q.removeRepeatableByKey(ctx, name, key)
}

func (q *Queue[D]) removeRepeatableByKey(ctx context.Context, name string, repeatJobKey string) error {
	checksum := utils.MD5Hash(fmt.Sprintf("%s::%s", name, utils.MD5Hash(repeatJobKey)))
	repeatJobIdPrefix := fmt.Sprintf("repeat:%s:", checksum)

	keys := []string{
		q.toKey("repeat"),
		q.toKey("delayed"),
	}
	result, err := lua.RemoveRepeatable(ctx, q.client, keys, repeatJobIdPrefix, repeatJobKey, q.keyPrefix)
	if err != nil {
		return fmt.Errorf("failed to remove repeatable: %w", err)
	}
	code, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected result type from removeRepeatable: %T", result)
	}
	if code == 1 {
		return fmt.Errorf("repeatable job with key %s not found", repeatJobKey)
	}
	return nil
}

// RetryJobsOpts configures the RetryJobs operation.
type RetryJobsOpts struct {
	State     string
	Count     int
	Timestamp int64
}

// RetryJobs moves jobs back to the wait state for re-processing.
func (q *Queue[D]) RetryJobs(ctx context.Context, opts RetryJobsOpts) error {
	if opts.State == "" {
		opts.State = "failed"
	}
	if opts.Count <= 0 {
		opts.Count = 1000
	}
	if opts.Timestamp <= 0 {
		opts.Timestamp = time.Now().UnixMilli()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		keys := []string{
			q.keyPrefix,
			q.toKey("events"),
			q.toKey(opts.State),
			q.toKey("wait"),
			q.toKey("paused"),
			q.toKey("meta"),
		}
		result, err := lua.MoveJobsToWait(ctx, q.client, keys, opts.Count, opts.Timestamp, opts.State)
		if err != nil {
			return fmt.Errorf("failed to retry jobs: %w", err)
		}
		code, ok := result.(int64)
		if !ok || code == 0 {
			break
		}
	}
	return nil
}

// PromoteJobs promotes all delayed jobs to the waiting state.
func (q *Queue[D]) PromoteJobs(ctx context.Context) error {
	delayedKey := q.toKey("delayed")
	jobIds, err := q.client.ZRange(ctx, delayedKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to get delayed jobs: %w", err)
	}
	var errs []error
	for _, jobId := range jobIds {
		if err := jobPromote(ctx, q.client, q.keyPrefix, jobId); err != nil {
			q.Emit("error", fmt.Sprintf("Failed to promote job %s: %v", jobId, err))
			errs = append(errs, fmt.Errorf("job %s: %w", jobId, err))
		}
	}
	return errors.Join(errs...)
}

// JobLogEntry holds a paginated slice of log entries and the total count.
type JobLogEntry struct {
	Logs  []string
	Count int64
}

// GetJobLogs retrieves log entries for a job with pagination.
func (q *Queue[D]) GetJobLogs(ctx context.Context, jobId string, start, end int64) (JobLogEntry, error) {
	logs, count, err := jobGetLogs(ctx, q.client, q.keyPrefix, jobId, start, end)
	if err != nil {
		return JobLogEntry{}, err
	}
	return JobLogEntry{Logs: logs, Count: count}, nil
}

// AddJobLog appends a log message to a job's log list.
func (q *Queue[D]) AddJobLog(ctx context.Context, jobId string, message string) (int64, error) {
	return jobLog(ctx, q.client, q.keyPrefix, jobId, message)
}

// MetricsResult holds queue metric data.
type MetricsResult struct {
	Meta  map[string]string
	Count int
}

// GetMetrics returns metrics for the specified type (e.g., "completed", "failed").
func (q *Queue[D]) GetMetrics(ctx context.Context, metricsType string) (MetricsResult, error) {
	metricsKey := q.toKey("metrics:" + metricsType)
	data, err := q.client.HGetAll(ctx, metricsKey).Result()
	if err != nil {
		return MetricsResult{}, fmt.Errorf("failed to get metrics: %w", err)
	}
	return MetricsResult{Meta: data, Count: len(data)}, nil
}

// GetRateLimitTtl returns the remaining TTL in milliseconds for the rate limiter.
func (q *Queue[D]) GetRateLimitTtl(ctx context.Context) (int64, error) {
	limiterKey := q.toKey("limiter")
	ttl, err := q.client.PTTL(ctx, limiterKey).Result()
	if err != nil {
		return 0, err
	}
	if ttl < 0 {
		return 0, nil
	}
	return ttl.Milliseconds(), nil
}

// DependencyInfo holds processed and unprocessed dependency data for a parent job.
type DependencyInfo struct {
	Processed   map[string]string
	Unprocessed []string
}

// GetDependencies returns both processed and unprocessed dependencies for a job.
func (q *Queue[D]) GetDependencies(ctx context.Context, jobId string) (DependencyInfo, error) {
	jobKey := q.keyPrefix + jobId
	pipe := q.client.Pipeline()
	processedCmd := pipe.HGetAll(ctx, jobKey+":processed")
	unprocessedCmd := pipe.SMembers(ctx, jobKey+":dependencies")
	_, err := pipe.Exec(ctx)
	if err != nil {
		return DependencyInfo{}, fmt.Errorf("failed to get dependencies: %w", err)
	}
	return DependencyInfo{
		Processed:   processedCmd.Val(),
		Unprocessed: unprocessedCmd.Val(),
	}, nil
}

// GetChildrenValues returns the return values of processed child jobs.
func (q *Queue[D]) GetChildrenValues(ctx context.Context, jobId string) (map[string]string, error) {
	deps, err := q.GetDependencies(ctx, jobId)
	if err != nil {
		return nil, err
	}
	return deps.Processed, nil
}
