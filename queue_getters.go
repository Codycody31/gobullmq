package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
)

// allStates is the default set of states for JobCounts, by state name.
// State names are mapped to their Redis key names (JobState.redisKey) at the
// Lua boundary.
var allStates = []JobState{
	JobStateWaiting,
	JobStateActive,
	JobStateCompleted,
	JobStateFailed,
	JobStateDelayed,
	JobStatePaused,
	JobStatePrioritized,
	JobStateWaitingChildren,
}

// Job retrieves a job by its ID.
func (q *Queue[D]) Job(ctx context.Context, jobId string) (*Job[D], error) {
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

// sanitizeJobStates mirrors upstream sanitizeJobTypes: nil/empty means all
// states, duplicates are dropped, and requesting the waiting state also
// includes paused (paused queues hold their waiting jobs there).
func sanitizeJobStates(states []JobState) []JobState {
	if len(states) == 0 {
		return allStates
	}
	seen := make(map[JobState]bool, len(states)+1)
	sanitized := make([]JobState, 0, len(states)+1)
	for _, s := range states {
		if !seen[s] {
			seen[s] = true
			sanitized = append(sanitized, s)
		}
	}
	if seen[JobStateWaiting] && !seen[JobStatePaused] {
		sanitized = append(sanitized, JobStatePaused)
	}
	return sanitized
}

// JobCounts returns counts for the specified states, keyed by the requested
// state names. Like upstream sanitizeJobTypes, requesting JobStateWaiting also
// counts JobStatePaused, and no states means all states.
func (q *Queue[D]) JobCounts(ctx context.Context, states ...JobState) (map[JobState]int64, error) {
	sanitized := sanitizeJobStates(states)
	requestedWaiting := false
	explicitPaused := false
	for _, state := range states {
		requestedWaiting = requestedWaiting || state == JobStateWaiting
		explicitPaused = explicitPaused || state == JobStatePaused
	}
	keys := []string{q.keyPrefix}
	args := make([]any, len(sanitized))
	for i, s := range sanitized {
		args[i] = s.redisKey() // key name (upstream getCountsArgs)
	}
	result, err := lua.GetCounts(ctx, q.client, keys, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get job counts: %w", err)
	}
	counts := make(map[JobState]int64, len(sanitized))
	if resultSlice, ok := result.([]any); ok {
		for i, s := range sanitized {
			if i < len(resultSlice) {
				if v, ok := resultSlice[i].(int64); ok {
					counts[s] = v
				}
			}
		}
	}
	// Paused is queried implicitly for a requested waiting count. Fold that
	// implementation detail back into the requested key instead of returning
	// an extra, unrequested paused entry. When paused was explicitly requested,
	// keep both state counts separate so aggregate callers do not double-count.
	if requestedWaiting && !explicitPaused {
		counts[JobStateWaiting] += counts[JobStatePaused]
		delete(counts, JobStatePaused)
	}
	return counts, nil
}

// Jobs returns jobs for the specified states with pagination. A nil or empty
// states slice means all states, and requesting JobStateWaiting also includes
// JobStatePaused, like upstream getJobs. Job ids appearing in several states
// are returned once.
func (q *Queue[D]) Jobs(ctx context.Context, states []JobState, start, end int, order SortOrder) ([]*Job[D], error) {
	sanitized := sanitizeJobStates(states)
	keys := []string{q.keyPrefix}
	asc := order == SortAsc
	ascStr := "0"
	if asc {
		ascStr = "1"
	}
	args := []any{start, end, ascStr}
	for _, s := range sanitized {
		args = append(args, s.redisKey())
	}
	result, err := lua.GetRanges(ctx, q.client, keys, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs: %w", err)
	}

	var jobs []*Job[D]
	resultSlice, ok := result.([]any)
	if !ok {
		return jobs, nil
	}

	// Collect ids in order, reversing list-backed results when ascending
	// (upstream getRanges reverses lrange responses when asc) and deduping
	// across states (upstream's Set).
	seen := make(map[string]bool)
	var ids []string
	for i, stateResult := range resultSlice {
		if i >= len(sanitized) {
			break
		}
		jobIds, ok := stateResult.([]any)
		if !ok {
			continue
		}
		listBacked := false
		switch sanitized[i].redisKey() {
		case "active", "wait", "paused":
			listBacked = true
		}
		for k := range jobIds {
			idx := k
			if asc && listBacked {
				idx = len(jobIds) - 1 - k
			}
			idStr := fmt.Sprintf("%v", jobIds[idx])
			if idStr == "" || strings.HasPrefix(idStr, "0:") || seen[idStr] {
				continue
			}
			seen[idStr] = true
			ids = append(ids, idStr)
		}
	}

	var errs []error
	for _, idStr := range ids {
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
	if len(errs) > 0 {
		q.emit("error", fmt.Errorf("jobs encountered %d errors loading jobs", len(errs)))
		return jobs, errors.Join(errs...)
	}
	return jobs, nil
}

// JobState returns the state of a specific job.
func (q *Queue[D]) JobState(ctx context.Context, jobId string) (JobState, error) {
	return jobGetState(ctx, q.client, q.keyPrefix, jobId)
}

// --- State-specific getters ---

// ActiveJobs returns jobs currently being processed.
func (q *Queue[D]) ActiveJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateActive}, start, end, SortAsc)
}

// CompletedJobs returns jobs that have been successfully completed.
func (q *Queue[D]) CompletedJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateCompleted}, start, end, SortDesc)
}

// DelayedJobs returns jobs that are scheduled to run after a delay.
func (q *Queue[D]) DelayedJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateDelayed}, start, end, SortAsc)
}

// FailedJobs returns jobs that have failed processing.
func (q *Queue[D]) FailedJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateFailed}, start, end, SortDesc)
}

// WaitingJobs returns jobs waiting to be processed (including jobs held on
// the paused list, like upstream getWaiting).
func (q *Queue[D]) WaitingJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateWaiting}, start, end, SortAsc)
}

// PrioritizedJobs returns jobs ordered by priority.
func (q *Queue[D]) PrioritizedJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStatePrioritized}, start, end, SortAsc)
}

// WaitingChildrenJobs returns parent jobs waiting for their children to complete.
func (q *Queue[D]) WaitingChildrenJobs(ctx context.Context, start, end int) ([]*Job[D], error) {
	return q.Jobs(ctx, []JobState{JobStateWaitingChildren}, start, end, SortAsc)
}

// --- State-specific counters ---

// ActiveCount returns the number of jobs currently being processed.
func (q *Queue[D]) ActiveCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateActive)
}

// CompletedCount returns the number of completed jobs.
func (q *Queue[D]) CompletedCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateCompleted)
}

// DelayedCount returns the number of delayed jobs.
func (q *Queue[D]) DelayedCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateDelayed)
}

// FailedCount returns the number of failed jobs.
func (q *Queue[D]) FailedCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateFailed)
}

// WaitingCount returns the number of jobs waiting to be processed, including
// jobs held on the paused list (upstream getWaitingCount).
func (q *Queue[D]) WaitingCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateWaiting)
}

// PrioritizedCount returns the number of prioritized jobs.
func (q *Queue[D]) PrioritizedCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStatePrioritized)
}

// WaitingChildrenCount returns the number of parent jobs waiting for children.
func (q *Queue[D]) WaitingChildrenCount(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx, JobStateWaitingChildren)
}

// Count returns the number of jobs waiting to be processed: waiting, paused,
// delayed, prioritized, or waiting-children (upstream count()).
func (q *Queue[D]) Count(ctx context.Context) (int64, error) {
	return q.JobCountByStates(ctx,
		JobStateWaiting,
		JobStatePaused,
		JobStateDelayed,
		JobStatePrioritized,
		JobStateWaitingChildren,
	)
}

// AddBulk adds multiple jobs atomically in a single transaction, like
// upstream addBulk. Queue defaults are merged under each job's Opts and
// Options; repeat options are stored but not scheduled, matching upstream
// createBulk.
func (q *Queue[D]) AddBulk(ctx context.Context, jobs []BulkJob[D]) ([]*Job[D], error) {
	if q.closed.Load() {
		return nil, ErrQueueClosed
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	q.ensureMeta(ctx)

	// Load the script up-front: EVALSHA inside MULTI cannot fall back on NOSCRIPT.
	if err := lua.AddJobScript.Load(ctx, q.client).Err(); err != nil {
		return nil, fmt.Errorf("failed to load addJob script: %w", err)
	}

	type bulkCommand struct {
		raw  *rawJob
		keys []string
		argv []any
	}
	commands := make([]bulkCommand, 0, len(jobs))
	parentKeys := make([]string, 0, len(jobs))
	parentIDs := make(map[string]string)
	for _, bj := range jobs {
		opts := mergeBulkJobOptions(q.defaultJobOptions, bj.Opts, bj.Options)
		if opts.JobID == "0" || strings.HasPrefix(opts.JobID, "0:") {
			return nil, fmt.Errorf("jobId cannot be '0' or start with '0:'")
		}
		jsonData, err := json.Marshal(bj.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data for bulk job %q: %w", bj.Name, err)
		}
		raw, err := newJob(bj.Name, string(jsonData), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create bulk job %q: %w", bj.Name, err)
		}
		keys, argv, err := buildAddJobArgs(q.keyPrefix, raw, opts.JobID)
		if err != nil {
			return nil, err
		}
		r := raw
		commands = append(commands, bulkCommand{raw: &r, keys: keys, argv: argv})
		if opts.Parent != nil {
			parentKey := opts.Parent.Queue + ":" + opts.Parent.ID
			if _, exists := parentIDs[parentKey]; !exists {
				parentKeys = append(parentKeys, parentKey)
				parentIDs[parentKey] = opts.Parent.ID
			}
		}
	}

	var ids []string
	runBatch := func(pipe redis.Pipeliner) error {
		cmds := make([]*redis.Cmd, 0, len(commands))
		for _, command := range commands {
			cmds = append(cmds, lua.AddJobScript.Run(ctx, pipe, command.keys, command.argv...))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to add jobs in bulk: %w", err)
		}
		parsed := make([]string, len(cmds))
		for i, cmd := range cmds {
			value, err := cmd.Result()
			if err != nil {
				return fmt.Errorf("failed to add bulk job %q: %w", jobs[i].Name, err)
			}
			parsed[i], err = parseAddJobResult(value)
			if err != nil {
				return err
			}
		}
		ids = parsed
		return nil
	}

	if len(parentKeys) == 0 {
		if err := runBatch(q.client.TxPipeline()); err != nil {
			return nil, err
		}
	} else {
		watcher, ok := q.client.(interface {
			Watch(context.Context, func(*redis.Tx) error, ...string) error
		})
		if !ok {
			return nil, fmt.Errorf("redis client does not support WATCH required for atomic AddBulk parent validation")
		}
		var err error
		for attempt := 0; attempt < 16; attempt++ {
			err = watcher.Watch(ctx, func(tx *redis.Tx) error {
				for _, parentKey := range parentKeys {
					exists, existsErr := tx.Exists(ctx, parentKey).Result()
					if existsErr != nil {
						return existsErr
					}
					if exists == 0 {
						return fmt.Errorf("bullmq: missing key for parent job %s while executing addJob", parentIDs[parentKey])
					}
				}
				return runBatch(tx.TxPipeline())
			}, parentKeys...)
			if !errors.Is(err, redis.TxFailedErr) {
				break
			}
		}
		if err != nil {
			return nil, err
		}
	}

	results := make([]*Job[D], 0, len(jobs))
	for i, command := range commands {
		command.raw.id = ids[i]
		command.raw.setJobContext(q.client, q.keyPrefix)
		results = append(results, &Job[D]{raw: command.raw, data: jobs[i].Data})
	}
	return results, nil
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

// RepeatableJobs returns repeatable job entries with pagination and sort order.
func (q *Queue[D]) RepeatableJobs(ctx context.Context, start, end int, order SortOrder) ([]RepeatableJobInfo, error) {
	key := q.toKey("repeat")
	var members []struct {
		Score  float64
		Member string
	}

	var zSlice []redis.Z
	var err error
	if order == SortAsc {
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

// parseRepeatKey mirrors upstream keyToData: name:jobId:endDate:tz:suffix,
// where the suffix (everything after the fourth colon) is either the cron
// pattern or the numeric every interval.
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
		if _, err := strconv.ParseInt(parts[4], 10, 64); err == nil {
			info.Every = parts[4]
		} else {
			info.Pattern = parts[4]
		}
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
	return q.removeRepeatableByKey(ctx, name, repeatOpts.JobID, repeatJobKey)
}

// RemoveRepeatableByKey removes a repeatable job by its unique key.
func (q *Queue[D]) RemoveRepeatableByKey(ctx context.Context, key string) error {
	data := parseRepeatKey(key)
	return q.removeRepeatableByKey(ctx, data.Name, data.ID, key)
}

func (q *Queue[D]) removeRepeatableByKey(ctx context.Context, name string, jobId string, repeatJobKey string) error {
	repeatJobId := repeat.GetJobId(name, "", utils.MD5Hash(repeatJobKey), jobId)
	legacyRepeatJobId := repeat.GetLegacyJobId(name, "", utils.MD5Hash(repeatJobKey), jobId)

	keys := []string{
		q.toKey("repeat"),
		q.toKey("delayed"),
	}
	result, err := lua.RemoveRepeatableCompat(ctx, q.client, keys, repeatJobId, legacyRepeatJobId, repeatJobKey, q.keyPrefix)
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

// RetryJobsOptions configures the RetryJobs operation.
type RetryJobsOptions struct {
	// State selects the finished set to re-process; it must be
	// JobStateFailed (the default) or JobStateCompleted.
	State     JobState
	Count     int
	Timestamp int64
}

// RetryJobs moves jobs back to the wait state for re-processing.
func (q *Queue[D]) RetryJobs(ctx context.Context, opts RetryJobsOptions) error {
	switch opts.State {
	case "":
		opts.State = JobStateFailed
	case JobStateFailed, JobStateCompleted:
	default:
		return fmt.Errorf("bullmq: RetryJobs state must be %q or %q, got %q", JobStateFailed, JobStateCompleted, opts.State)
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
			q.toKey(opts.State.redisKey()),
			q.toKey("wait"),
			q.toKey("paused"),
			q.toKey("meta"),
		}
		result, err := lua.MoveJobsToWait(ctx, q.client, keys, opts.Count, opts.Timestamp, opts.State.redisKey())
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

// PromoteJobs promotes all delayed jobs to the waiting state, batching
// through the moveJobsToWait script with a MAX_SAFE_INTEGER timestamp like
// upstream promoteJobs.
func (q *Queue[D]) PromoteJobs(ctx context.Context) error {
	// Upstream sends Number.MAX_VALUE as the promotion cutoff; the Lua side
	// tonumber()s it, so send the identical literal.
	const maxTimestamp = "1.7976931348623157e+308"
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		keys := []string{
			q.keyPrefix,
			q.toKey("events"),
			q.toKey("delayed"),
			q.toKey("wait"),
			q.toKey("paused"),
			q.toKey("meta"),
		}
		result, err := lua.MoveJobsToWait(ctx, q.client, keys, 1000, maxTimestamp, "delayed")
		if err != nil {
			return fmt.Errorf("failed to promote jobs: %w", err)
		}
		code, ok := result.(int64)
		if !ok || code == 0 {
			break
		}
	}
	return nil
}

// JobLogs retrieves log entries for a job with pagination. total is the full
// length of the job's log list. SortDesc returns the entries counted from the
// newest end, newest first (upstream getJobLogs asc=false).
func (q *Queue[D]) JobLogs(ctx context.Context, jobId string, start, end int64, order SortOrder) (logs []string, total int64, err error) {
	return jobGetLogs(ctx, q.client, q.keyPrefix, jobId, start, end, order)
}

// AddJobLog appends a log message to a job's log list, keeping only the most
// recent keepLogs entries when keepLogs > 0 (0 keeps all), like upstream
// Queue.addJobLog.
func (q *Queue[D]) AddJobLog(ctx context.Context, jobId string, message string, keepLogs int64) (int64, error) {
	return jobLog(ctx, q.client, q.keyPrefix, jobId, message, keepLogs)
}

// MetricsMeta holds the bookkeeping fields of a metrics hash.
type MetricsMeta struct {
	Count     int64
	PrevTS    int64
	PrevCount int64
}

// MetricsResult holds queue metric data: job counts per minute (Data,
// newest first) and the number of points kept (Count).
type MetricsResult struct {
	Meta  MetricsMeta
	Data  []string
	Count int64
}

// Metrics returns gathered metrics for the given type ("completed" or
// "failed") like upstream getMetrics. start and end select data points, where
// 0 is the newest and -1 the oldest.
func (q *Queue[D]) Metrics(ctx context.Context, metricsType string, start, end int64) (MetricsResult, error) {
	metricsKey := q.toKey("metrics:" + metricsType)
	dataKey := metricsKey + ":data"

	pipe := q.client.Pipeline()
	metaCmd := pipe.HMGet(ctx, metricsKey, "count", "prevTS", "prevCount")
	dataCmd := pipe.LRange(ctx, dataKey, start, end)
	lenCmd := pipe.LLen(ctx, dataKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return MetricsResult{}, fmt.Errorf("failed to get metrics: %w", err)
	}

	meta := metaCmd.Val()
	metaInt := func(i int) int64 {
		if i < len(meta) {
			if s, ok := meta[i].(string); ok {
				if n, err := strconv.ParseInt(s, 10, 64); err == nil {
					return n
				}
			}
		}
		return 0
	}

	return MetricsResult{
		Meta: MetricsMeta{
			Count:     metaInt(0),
			PrevTS:    metaInt(1),
			PrevCount: metaInt(2),
		},
		Data:  dataCmd.Val(),
		Count: lenCmd.Val(),
	}, nil
}

// RateLimitTTL returns the remaining time to live of the rate limiter key,
// straight from PTTL: -1ms means the key exists but has no expiry, -2ms means
// the key does not exist.
func (q *Queue[D]) RateLimitTTL(ctx context.Context) (time.Duration, error) {
	limiterKey := q.toKey("limiter")
	ttl, err := q.client.PTTL(ctx, limiterKey).Result()
	if err != nil {
		return 0, err
	}
	if ttl < 0 {
		// go-redis reports the PTTL sentinels as raw -1/-2 durations
		// (nanoseconds); scale them to milliseconds like the Redis reply.
		return ttl * time.Millisecond, nil
	}
	return ttl, nil
}

// DependencyInfo holds processed and unprocessed dependency data for a parent job.
type DependencyInfo struct {
	Processed   map[string]string
	Unprocessed []string
}

// Dependencies returns both processed and unprocessed dependencies for a job.
func (q *Queue[D]) Dependencies(ctx context.Context, jobId string) (DependencyInfo, error) {
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

// ChildrenValues returns the decoded return values of processed child jobs.
func (q *Queue[D]) ChildrenValues(ctx context.Context, jobId string) (map[string]any, error) {
	deps, err := q.Dependencies(ctx, jobId)
	if err != nil {
		return nil, err
	}
	values := make(map[string]any, len(deps.Processed))
	for key, value := range deps.Processed {
		values[key] = parseReturnValue(value)
	}
	return values, nil
}

// JobCountByStates returns the total number of jobs across the given
// states (all states when none are given), mirroring upstream
// getJobCountByTypes.
func (q *Queue[D]) JobCountByStates(ctx context.Context, states ...JobState) (int64, error) {
	counts, err := q.JobCounts(ctx, states...)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	return total, nil
}

// UpdateJobProgress updates a job's progress and emits the progress event on
// the queue events stream.
func (q *Queue[D]) UpdateJobProgress(ctx context.Context, jobId string, progress any) error {
	return jobUpdateProgress(ctx, q.client, q.keyPrefix, jobId, progress)
}

// RemoveDeprecatedPriorityKey deletes the legacy 'priority' zset used before
// BullMQ v4's prioritized set.
func (q *Queue[D]) RemoveDeprecatedPriorityKey(ctx context.Context) error {
	return q.client.Del(ctx, q.toKey("priority")).Err()
}

// Workers returns the Redis clients (from CLIENT LIST) registered as
// workers for this queue via their connection name.
// Note: some managed Redis providers do not support CLIENT SETNAME.
func (q *Queue[D]) Workers(ctx context.Context) ([]map[string]string, error) {
	return q.baseGetClients(ctx, "")
}

// EventListeners returns the Redis clients registered as QueueEvents
// listeners for this queue.
func (q *Queue[D]) EventListeners(ctx context.Context) ([]map[string]string, error) {
	return q.baseGetClients(ctx, ":qe")
}

func (q *Queue[D]) baseGetClients(ctx context.Context, suffix string) ([]map[string]string, error) {
	list, err := q.client.ClientList(ctx).Result()
	if err != nil {
		return nil, err
	}
	target := q.prefix + ":" + base64.StdEncoding.EncodeToString([]byte(q.name)) + suffix
	var clients []map[string]string
	for _, line := range strings.Split(list, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		info := make(map[string]string)
		for _, kv := range strings.Split(line, " ") {
			if i := strings.Index(kv, "="); i >= 0 {
				info[kv[:i]] = kv[i+1:]
			}
		}
		if info["name"] == target {
			clients = append(clients, info)
		}
	}
	return clients, nil
}
