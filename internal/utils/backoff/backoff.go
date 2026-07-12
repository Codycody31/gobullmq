package backoff

import (
	"fmt"
	"math"
)

// Options defines the backoff strategy and base delay (in milliseconds).
type Options struct {
	Type  string `json:"type" msgpack:"type"`
	Delay int    `json:"delay" msgpack:"delay"`
}

// IsBuiltin reports whether the type is handled by Calculate ("" means no
// backoff). Non-builtin types are routed to custom strategies, mirroring
// upstream lookupStrategy's precedence.
func IsBuiltin(backoffType string) bool {
	switch backoffType {
	case "", "fixed", "exponential":
		return true
	}
	return false
}

// Calculate computes the backoff delay in milliseconds, mirroring upstream
// Backoffs.calculate. Returning 0 means retry immediately; a positive value
// schedules a delayed retry. An unknown strategy name is an error, matching
// upstream's "Unknown backoff strategy" throw.
func Calculate(opts Options, attemptsMade int) (int, error) {
	switch opts.Type {
	case "":
		// No backoff configured: retry immediately.
		return 0, nil
	case "fixed":
		return opts.Delay, nil
	case "exponential":
		return int(math.Round(math.Pow(2, float64(attemptsMade-1)) * float64(opts.Delay))), nil
	default:
		return 0, fmt.Errorf("unknown backoff strategy %s. If a custom backoff strategy is used, specify it via WorkerOptions.BackoffStrategy", opts.Type)
	}
}
