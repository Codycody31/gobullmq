package backoff

import (
	"math"
)

// Options defines the backoff strategy and base delay (in milliseconds).
type Options struct {
	Type  string `json:"type" msgpack:"type"`
	Delay int    `json:"delay" msgpack:"delay"`
}

// Strategy is a function that returns a delay in milliseconds based on attemptsMade.
type Strategy func(attemptsMade int, backoffType string) int

// Built-in strategies mapped by name.
var builtinStrategies = map[string]func(delay int) Strategy{
	"fixed": func(delay int) Strategy {
		return func(_ int, _ string) int {
			return max(0, delay)
		}
	},
	"exponential": func(delay int) Strategy {
		return func(attemptsMade int, _ string) int {
			if attemptsMade <= 0 {
				attemptsMade = 1
			}
			factor := math.Pow(2, float64(attemptsMade-1))
			d := int(math.Round(factor * float64(delay)))
			if d < 0 {
				return 0
			}
			return d
		}
	},
}

// Calculate computes the backoff delay in milliseconds for the given options and attemptsMade.
// Returning 0 means retry immediately. Returning a positive value schedules a delayed retry.
// Returning -1 can be used by custom strategies to signal "do not retry".
func Calculate(opts Options, attemptsMade int) int {
	if opts.Type == "" {
		return 0
	}
	factory, ok := builtinStrategies[opts.Type]
	if !ok {
		// Unknown strategy -> default to immediate retry
		return 0
	}
	strategy := factory(opts.Delay)
	return strategy(attemptsMade, opts.Type)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
