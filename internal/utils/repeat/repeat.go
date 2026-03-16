package repeat

import (
	"fmt"
	"strconv"
	"time"

	"go.codycody31.dev/gobullmq/internal/utils"
)

// GetJobId returns the job id
func GetJobId(name string, nextMillis int64, namespace string, jobId string) (string, error) {
	checksum := utils.MD5Hash(fmt.Sprintf("%s:%s:%s", name, jobId, namespace))
	return fmt.Sprintf("repeat:%s:%s", checksum, strconv.FormatInt(nextMillis, 10)), nil
}

// RepeatKeyOpts holds the fields needed to build a repeat key.
type RepeatKeyOpts struct {
	EndDate *time.Time
	TZ      string
	Pattern string
	Every   int
	JobId   string
}

// GetKey returns the key for the repeatable job
func GetKey(name string, opts RepeatKeyOpts) string {
	var endDate string
	if opts.EndDate != nil {
		endDate = strconv.FormatInt(opts.EndDate.UnixNano()/int64(time.Millisecond), 10)
	} else {
		endDate = ""
	}

	tz := opts.TZ
	pattern := opts.Pattern
	suffix := pattern
	if suffix == "" {
		suffix = strconv.Itoa(opts.Every)
	}

	jobId := opts.JobId

	return fmt.Sprintf("%s:%s:%s:%s:%s", name, jobId, endDate, tz, suffix)
}
