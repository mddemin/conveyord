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
