package repeat

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/gorhill/cronexpr"
	"go.codycody31.dev/gobullmq/internal/utils"
	"go.codycody31.dev/gobullmq/types"
)

// GetJobId returns the job id
func GetJobId(name string, nextMillis int64, namespace string, jobId string) (string, error) {
	checksum := utils.MD5Hash(fmt.Sprintf("%s:%s:%s", name, jobId, namespace))
	return fmt.Sprintf("repeat:%s:%s", checksum, strconv.FormatInt(nextMillis, 10)), nil
}

// GetKey returns the key for the repeatable job
func GetKey(name string, repeat types.JobRepeatOptions) string {
	var endDate string
	if repeat.EndDate != nil {
		endDate = strconv.FormatInt(repeat.EndDate.UnixNano()/int64(time.Millisecond), 10)
	} else {
		endDate = ""
	}

	tz := repeat.TZ
	pattern := repeat.Pattern
	suffix := pattern
	if suffix == "" {
		suffix = strconv.Itoa(repeat.Every)
	}

	jobId := repeat.JobId

	return fmt.Sprintf("%s:%s:%s:%s:%s", name, jobId, endDate, tz, suffix)
}

// Strategy returns the next time for the repeatable job
func Strategy(millis int64, opts types.JobOptions) (int64, error) {
	ropts := opts.Repeat
	pattern := ropts.Pattern

	if pattern != "" && ropts.Every != 0 {
		return 0, fmt.Errorf("both .pattern and .every options are defined for this repeatable job")
	}

	if ropts.Every != 0 {
		expr := math.Floor(float64(millis/int64(ropts.Every)))*float64(ropts.Every) + func() float64 {
			if ropts.Immediately {
				return 0
			}
			return float64(ropts.Every)
		}()
		return int64(expr), nil
	}

	// Calc currentData based of opts
	var currentDate time.Time
	if !ropts.StartDate.IsZero() {
		startDate, _ := time.Parse(time.RFC3339, ropts.StartDate.String())
		if startDate.After(time.Unix(millis/1000, 0)) {
			currentDate = startDate
		} else {
			currentDate = time.Unix(millis/1000, 0)
		}
	} else {
		currentDate = time.Unix(millis/1000, 0)
	}

	// Get interval next time
	nextTime := cronexpr.MustParse(pattern).Next(currentDate)

	return nextTime.UnixNano() / int64(time.Millisecond), nil
}
