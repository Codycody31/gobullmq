package gobullmq

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/internal/utils/repeat"
)

// 'every' intervals snap to the interval boundary like upstream getNextMillis.
func TestGetNextMillisEverySnapsToBoundary(t *testing.T) {
	got, err := getNextMillis(1_000_123, &JobRepeatOptions{Every: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if got != 1_020_000 {
		t.Fatalf("getNextMillis = %d, want 1020000 (floor snap + every)", got)
	}

	// immediately: no +every offset.
	got, err = getNextMillis(1_000_123, &JobRepeatOptions{Every: 60_000, Immediately: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != 960_000 {
		t.Fatalf("getNextMillis immediately = %d, want 960000", got)
	}
}

// Defining both pattern and every is an error upstream.
func TestGetNextMillisRejectsPatternAndEvery(t *testing.T) {
	if _, err := getNextMillis(0, &JobRepeatOptions{Every: 1000, Pattern: "* * * * *"}); err == nil {
		t.Fatal("pattern+every accepted; upstream throws")
	}
}

// Standard 5-field cron patterns must parse.
func TestGetNextMillisFiveFieldCron(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 1, 0, time.UTC).UnixMilli()
	got, err := getNextMillis(base, &JobRepeatOptions{Pattern: "*/5 * * * *", UTC: true})
	if err != nil {
		t.Fatalf("5-field cron rejected: %v", err)
	}
	want := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC).UnixMilli()
	if got != want {
		t.Fatalf("next = %d, want %d", got, want)
	}
	// 6-field (with seconds) still accepted.
	if _, err := getNextMillis(base, &JobRepeatOptions{Pattern: "0 */5 * * * *", UTC: true}); err != nil {
		t.Fatalf("6-field cron rejected: %v", err)
	}
}

// Cron chains end when the next occurrence falls past endDate (cron-parser
// enforces endDate inside its iterator upstream), and an invalid tz errors.
func TestGetNextMillisCronEndDateAndTZ(t *testing.T) {
	now := time.Now().UnixMilli()
	// Yearly pattern with endDate tomorrow: no occurrence is due.
	got, err := getNextMillis(now, &JobRepeatOptions{Pattern: "0 0 1 1 *", UTC: true, EndDate: now + 86_400_000})
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("occurrence past endDate scheduled: %d", got)
	}
	if _, err := getNextMillis(now, &JobRepeatOptions{Pattern: "* * * * *", TZ: "Not/AZone"}); err == nil {
		t.Fatal("invalid tz accepted; upstream throws")
	}
}

// The repeat offset persists across iterations, keeping every+immediately
// chains at slot+offset like upstream's { offset, ...filteredRepeatOpts }.
func TestRepeatOffsetPersistsAcrossIterations(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := q.Add(ctx, "rep", map[string]any{}, AddWithRepeat(JobRepeatOptions{Every: 60_000, Immediately: true}))
	if err != nil {
		t.Fatal(err)
	}

	readOffset := func(id string) float64 {
		optsJSON, err := c.HGet(ctx, prefix+id, "opts").Result()
		if err != nil {
			t.Fatalf("opts for %s: %v", id, err)
		}
		var opts map[string]any
		if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
			t.Fatal(err)
		}
		rep, _ := opts["repeat"].(map[string]any)
		off, _ := rep["offset"].(float64)
		return off
	}
	firstOffset := readOffset(first.ID())
	if firstOffset == 0 {
		t.Fatal("immediately iteration stored no offset")
	}

	// Fetch iteration #1 (immediately -> already waiting); the scheduler must
	// carry the stored offset into iteration #2's opts.
	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.moveToActive(ctx, w.token.String()+":1", ""); err != nil {
		t.Fatal(err)
	}
	delayed, err := c.ZRange(ctx, prefix+"delayed", 0, -1).Result()
	if err != nil || len(delayed) != 1 {
		t.Fatalf("iteration #2 not scheduled: %v (%v)", delayed, err)
	}
	if got := readOffset(delayed[0]); got != firstOffset {
		t.Fatalf("iteration #2 offset = %v, want persisted %v", got, firstOffset)
	}
}

// JS producers may store repeat dates as ISO strings; the Go side must parse them.
func TestRepeatOptionsAcceptISODates(t *testing.T) {
	opts, err := jobOptsFromJson(`{"repeat":{"every":60000,"endDate":"2030-01-02T03:04:05.000Z"},"attempts":0,"delay":0}`)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()
	if opts.Repeat == nil || opts.Repeat.EndDate != want {
		t.Fatalf("endDate = %+v, want %d", opts.Repeat, want)
	}
	// Numeric form still works.
	opts, err = jobOptsFromJson(`{"repeat":{"every":60000,"endDate":1893553445000}}`)
	if err != nil || opts.Repeat.EndDate != 1893553445000 {
		t.Fatalf("numeric endDate = %+v (%v)", opts.Repeat, err)
	}
}

// Repeat job ids must be byte-compatible with JS md5(name + jobId + namespace).
// Vectors computed with node crypto against the upstream algorithm.
func TestRepeatJobIdMatchesJS(t *testing.T) {
	key := repeat.GetKey("foo", repeat.RepeatKeyOpts{Every: 60_000})
	if key != "foo::::60000" {
		t.Fatalf("repeat key = %q, want foo::::60000", key)
	}
	id := repeat.GetJobId("foo", "1020000", utils.MD5Hash(key), "")
	if id != "repeat:89eefeff1027a847fb82da7ea39bad4e:1020000" {
		t.Fatalf("repeat job id = %q, want JS-compatible checksum 89eefeff...", id)
	}

	key2 := repeat.GetKey("foo", repeat.RepeatKeyOpts{Every: 60_000, JobId: "myid"})
	if key2 != "foo:myid:::60000" {
		t.Fatalf("repeat key2 = %q, want foo:myid:::60000", key2)
	}
	id2 := repeat.GetJobId("foo", "1020000", utils.MD5Hash(key2), "myid")
	if id2 != "repeat:9207ce4cc4e3c63e40340c049f5209b2:1020000" {
		t.Fatalf("repeat job id2 = %q, want JS-compatible checksum 9207ce4cc...", id2)
	}
}

// PrevMillis must be stored top-level in the job opts, not nested under repeat.
func TestRepeatablePrevMillisStoredTopLevel(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	q, prefix := testQueue(t, c)

	job, err := q.Add(ctx, "rep", map[string]any{}, AddWithRepeat(JobRepeatOptions{Every: 60_000}))
	if err != nil {
		t.Fatal(err)
	}
	optsJSON, err := c.HGet(ctx, prefix+job.ID(), "opts").Result()
	if err != nil {
		t.Fatal(err)
	}
	var opts map[string]any
	if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
		t.Fatal(err)
	}
	pm, ok := opts["prevMillis"].(float64)
	if !ok || pm <= 0 {
		t.Fatalf("top-level prevMillis missing or invalid in %s", optsJSON)
	}
	rep, _ := opts["repeat"].(map[string]any)
	if _, has := rep["prevMillis"]; has {
		t.Fatalf("prevMillis nested under repeat in %s; upstream stores it top-level", optsJSON)
	}
	// The job id must embed prevMillis as the slot.
	if !strings.HasSuffix(job.ID(), ":"+strconv.FormatInt(int64(pm), 10)) {
		t.Fatalf("job id %q does not end with slot %d", job.ID(), int64(pm))
	}
}

// The next iteration is scheduled at fetch time, and only if the
// repeatable still exists in the repeat zset (so RemoveRepeatable stops the chain).
func TestRepeatChainScheduledOnFetchAndStoppableByRemoval(t *testing.T) {
	ctx := context.Background()
	c := testClient(t)
	name := testQueueName(t, c)
	prefix := "bull:" + name + ":"

	q, err := NewQueue[map[string]any](name, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	repeatOpts := JobRepeatOptions{Every: 60_000}
	first, err := q.Add(ctx, "rep", map[string]any{}, AddWithRepeat(repeatOpts))
	if err != nil {
		t.Fatal(err)
	}

	// The first instance is delayed; promote it into wait so a fetch can pick it up.
	if err := q.PromoteJobs(ctx); err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker[map[string]any, string](name, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.runCtx = ctx

	// Fetching the job (before any processing) must schedule iteration #2.
	got, err := w.moveToActive(ctx, w.token.String()+":1", "")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.id != first.ID() {
		t.Fatalf("fetched %+v, want job %s", got, first.ID())
	}
	delayed, err := c.ZRange(ctx, prefix+"delayed", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(delayed) != 1 || !strings.HasPrefix(delayed[0], "repeat:") {
		t.Fatalf("next iteration not scheduled on fetch: delayed=%v", delayed)
	}
	if delayed[0] == first.ID() {
		t.Fatalf("delayed job is the same iteration: %v", delayed)
	}

	// Removing the repeatable must stop the chain: promote iteration #2,
	// fetch it, and verify no iteration #3 appears.
	if err := q.RemoveRepeatable(ctx, "rep", repeatOpts); err == nil {
		// Removal also deletes the delayed iteration #2 (chain fully stopped).
		delayed, _ = c.ZRange(ctx, prefix+"delayed", 0, -1).Result()
		if len(delayed) != 0 {
			t.Fatalf("RemoveRepeatable left delayed iterations: %v", delayed)
		}
	} else {
		t.Fatalf("RemoveRepeatable: %v", err)
	}
	if n, _ := c.ZCard(ctx, prefix+"repeat").Result(); n != 0 {
		t.Fatalf("repeat zset not emptied by RemoveRepeatable")
	}
}

// ParseRepeatKey distinguishes numeric every from cron patterns and keeps
// colons inside patterns intact.
func TestParseRepeatKey(t *testing.T) {
	info := parseRepeatKey("myjob:custom:1700000000000:Europe/Paris:*/5 * * * *")
	if info.Name != "myjob" || info.ID != "custom" || info.EndDate != "1700000000000" {
		t.Fatalf("parsed %+v", info)
	}
	if info.TZ != "Europe/Paris" {
		t.Fatalf("TZ = %q", info.TZ)
	}
	if info.Pattern != "*/5 * * * *" || info.Every != "" {
		t.Fatalf("pattern/every misassigned: %+v", info)
	}

	info = parseRepeatKey("myjob::::60000")
	if info.Every != "60000" || info.Pattern != "" {
		t.Fatalf("numeric suffix misassigned: %+v", info)
	}
}
