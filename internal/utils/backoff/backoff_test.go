package backoff

import (
	"strings"
	"testing"
)

// Exact upstream Backoffs semantics: fixed returns delay, exponential is
// round(2^(attemptsMade-1) * delay) with no cap, unknown types error.
func TestCalculate(t *testing.T) {
	if d, err := Calculate(Options{Type: "fixed", Delay: 1500}, 3); err != nil || d != 1500 {
		t.Fatalf("fixed = %d, %v; want 1500, nil", d, err)
	}
	if d, err := Calculate(Options{Type: "exponential", Delay: 1000}, 3); err != nil || d != 4000 {
		t.Fatalf("exponential attempt 3 = %d, %v; want 4000, nil", d, err)
	}
	// No 24h cap: attempt 20 with 1s base = 2^19 * 1000 = 524288000ms (~6 days).
	if d, err := Calculate(Options{Type: "exponential", Delay: 1000}, 20); err != nil || d != 524_288_000 {
		t.Fatalf("exponential attempt 20 = %d, %v; want 524288000 (uncapped), nil", d, err)
	}
	// Empty type: no backoff configured, retry immediately.
	if d, err := Calculate(Options{}, 3); err != nil || d != 0 {
		t.Fatalf("empty type = %d, %v; want 0, nil", d, err)
	}
	// Unknown strategy must error, mirroring upstream's throw.
	if _, err := Calculate(Options{Type: "jitter", Delay: 100}, 1); err == nil || !strings.Contains(err.Error(), "jitter") {
		t.Fatalf("unknown strategy error = %v; want error naming the strategy", err)
	}
}
