package job

import (
	"errors"
	"fmt"
	"reflect"
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

func TestOnFailure(t *testing.T) {
	p := Policy{
		Base:        time.Second,
		Max:         time.Minute,
		MaxAttempts: 3,
		Jitter:      noJitter,
	}
	boom := errors.New("boom")

	tests := []struct {
		name    string
		attempt int
		cause   error
		want    Decision
	}{
		{
			name:    "first failure retries after Base",
			attempt: 1,
			cause:   boom,
			want:    Decision{State: Pending, Backoff: time.Second},
		},
		{
			name:    "second failure backs off further",
			attempt: 2,
			cause:   boom,
			want:    Decision{State: Pending, Backoff: 2 * time.Second},
		},
		{
			name:    "final attempt is buried",
			attempt: 3,
			cause:   boom,
			want:    Decision{State: Dead},
		},
		{
			name:    "beyond the limit stays buried",
			attempt: 9,
			cause:   boom,
			want:    Decision{State: Dead},
		},
		{
			name:    "permanent failure is buried on the first attempt",
			attempt: 1,
			cause:   Permanent(boom),
			want:    Decision{State: Dead},
		},
		{
			name:    "permanent failure wrapped in context is still buried",
			attempt: 1,
			cause:   fmt.Errorf("fetching url: %w", Permanent(boom)),
			want:    Decision{State: Dead},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{ID: "j1", State: Leased, Attempt: tt.attempt}

			got := p.OnFailure(j, tt.cause)
			if got != tt.want {
				t.Errorf("OnFailure() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestOnFailureDoesNotMutateTheJob(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 3, Jitter: noJitter}
	j := &Job{ID: "j1", State: Leased, Attempt: 1}
	before := *j

	p.OnFailure(j, errors.New("boom"))

	if !reflect.DeepEqual(*j, before) {
		t.Errorf("OnFailure mutated the job: got %+v, want %+v", *j, before)
	}
}

func TestOnFailureDecisionIsAppliable(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 3, Jitter: noJitter}
	j := &Job{ID: "j1", State: Leased, Attempt: 1}

	d := p.OnFailure(j, errors.New("boom"))
	if err := j.To(d.State); err != nil {
		t.Fatalf("applying decision %s to a Leased job: %v", d.State, err)
	}
}

func TestOnFailureMaxAttemptsZero(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 0, Jitter: noJitter}
	j := &Job{ID: "j1", State: Leased, Attempt: 1}

	d := p.OnFailure(j, errors.New("boom"))
	if d.State != Pending {
		t.Fatalf("Job should be Pending: %v", d.State)
	}
}

// Max is a cap on the window "however many attempts", including the first —
// where the doubling loop that applies the cap never runs. A Base larger than
// Max is a misconfiguration, and refusing to exceed the stated ceiling is the
// one thing that must still hold under one.
func TestBackoffNeverExceedsMax(t *testing.T) {
	p := Policy{Base: 10 * time.Second, Max: time.Second, Jitter: noJitter}

	for _, attempt := range []int{0, 1, 2, 3, 10} {
		if got := p.Backoff(attempt); got > p.Max {
			t.Errorf("Backoff(%d) = %v, want at most Max (%v)", attempt, got, p.Max)
		}
	}
}

// Max: 0 means "no cap", by the same reading as MaxAttempts: 0 meaning "no
// limit" — not "no delay". A Policy that sets Base and MaxAttempts but no Max
// must still back off; taking min(Base, 0) instead makes every retry immediate,
// which is a hot loop between the dispatcher and Postgres that burns the whole
// attempt budget as fast as the loop turns.
func TestAnUnsetMaxIsNoCapNotNoDelay(t *testing.T) {
	p := Policy{Base: time.Second, MaxAttempts: 3, Jitter: noJitter}

	for _, attempt := range []int{1, 2, 3} {
		if got := p.Backoff(attempt); got <= 0 {
			t.Errorf("Backoff(%d) = %v with no Max set, want a real delay", attempt, got)
		}
	}
}

// "No cap" still has to mean a duration. Doubling an unbounded window walks off
// the end of an int64 after about forty attempts, and time.Duration(float64)
// wraps to the negative extreme rather than saturating — so next_attempt_at
// becomes now() minus 292 years, the job is instantly leasable forever, and the
// hot loop the test above describes arrives anyway, just later and disguised.
// A job cannot reach attempt 40 today because MaxAttempts stops it, but nothing
// in Backoff says so, and MaxAttempts: 0 also means no limit.
//
// The trap in fixing it: float64(math.MaxInt64) is 2^63, one *more* than
// math.MaxInt64, because a float64 cannot represent 2^63-1. Clamping the float
// to it therefore still overflows the conversion. It is a ceiling to stay
// strictly under, not a value to clamp to.
func TestAnUnsetMaxDoesNotOverflow(t *testing.T) {
	p := Policy{Base: time.Second, Jitter: noJitter}

	for _, attempt := range []int{40, 64, 100} {
		if got := p.Backoff(attempt); got <= 0 {
			t.Errorf("Backoff(%d) = %v, want a positive duration", attempt, got)
		}
	}
}
