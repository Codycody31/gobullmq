package repeat

import (
	"fmt"
	"strconv"

	"go.codycody31.dev/gobullmq/internal/utils"
)

// GetJobId builds a repeatable job instance id, byte-compatible with
// upstream getRepeatJobId: md5(name + jobId + namespace) with no separators.
// nextMillis is a string so removal paths can pass "" (yielding the id prefix).
func GetJobId(name string, nextMillis string, namespace string, jobId string) string {
	checksum := utils.MD5Hash(name + jobId + namespace)
	return fmt.Sprintf("repeat:%s:%s", checksum, nextMillis)
}

// GetLegacyJobId builds the instance id used by pre-redesign Go releases.
// It is retained only for removing delayed iterations that survive an upgrade.
func GetLegacyJobId(name string, nextMillis string, namespace string, jobId string) string {
	checksum := utils.MD5Hash(fmt.Sprintf("%s:%s:%s", name, jobId, namespace))
	return fmt.Sprintf("repeat:%s:%s", checksum, nextMillis)
}

// RepeatKeyOpts holds the fields needed to build a repeat key.
type RepeatKeyOpts struct {
	EndDate int64 // Unix epoch milliseconds, 0 when unset
	TZ      string
	Pattern string
	Every   int
	JobId   string
}

// GetKey returns the key for the repeatable job, byte-compatible with
// upstream getRepeatKey: name:jobId:endDate:tz:suffix.
func GetKey(name string, opts RepeatKeyOpts) string {
	endDate := ""
	if opts.EndDate != 0 {
		endDate = strconv.FormatInt(opts.EndDate, 10)
	}

	suffix := opts.Pattern
	if suffix == "" {
		suffix = strconv.Itoa(opts.Every)
	}

	return fmt.Sprintf("%s:%s:%s:%s:%s", name, opts.JobId, endDate, opts.TZ, suffix)
}
