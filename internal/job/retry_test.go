package job

import (
	"strconv"
	"testing"
	"time"
)

// noJitter draws the maximum of the jitter range, so Backoff returns the
// ceiling of the window and the exponential sequence can be asserted exactly.
func noJitter() float64 { return 1 }

func TestBackoffCeilingSequence(t *testing.T) {
	p := Policy{
		Base:   100 * time.Millisecond,
		Max:    2 * time.Second,
		Jitter: noJitter,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 2 * time.Second}, // capped at Max
		{7, 2 * time.Second},
		{50, 2 * time.Second}, // no overflow at large attempt counts
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.attempt), func(t *testing.T) {
			if got := p.Backoff(tt.attempt); got != tt.want {
				t.Errorf("Backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestBackoffTreatsNonPositiveAttemptAsFirst(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second, Jitter: noJitter}

	for _, attempt := range []int{0, -1} {
		if got := p.Backoff(attempt); got != 100*time.Millisecond {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, 100*time.Millisecond)
		}
	}
}

func TestBackoffAppliesJitter(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Jitter: func() float64 { return 0.25 }}

	// The window for attempt 3 is 4s; a quarter of it is 1s.
	if got, want := p.Backoff(3), time.Second; got != want {
		t.Errorf("Backoff(3) = %v, want %v", got, want)
	}
}

func TestBackoffStaysWithinWindowWithRealRandomness(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second} // nil Jitter: real randomness

	const window = 400 * time.Millisecond // attempt 3
	seen := make(map[time.Duration]bool)

	for range 1000 {
		d := p.Backoff(3)
		if d < 0 || d >= window {
			t.Fatalf("Backoff(3) = %v, want within [0, %v)", d, window)
		}
		seen[d] = true
	}

	if len(seen) < 100 {
		t.Errorf("got %d distinct delays over 1000 draws, want at least 100 — jitter looks constant", len(seen))
	}
}
