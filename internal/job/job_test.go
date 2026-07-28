package job

import (
	"errors"
	"testing"
)

func TestJobToAdvancesState(t *testing.T) {
	j := &Job{ID: "j1", State: Pending}

	if err := j.To(Leased); err != nil {
		t.Fatalf("To(Leased) = %v, want nil", err)
	}
	if j.State != Leased {
		t.Errorf("State = %s, want %s", j.State, Leased)
	}
}

func TestJobToRejectsIllegalTransition(t *testing.T) {
	j := &Job{ID: "j1", State: Pending}

	err := j.To(Succeeded)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("To(Succeeded) = %v, want ErrInvalidTransition", err)
	}
	if j.State != Pending {
		t.Errorf("State = %s after rejected transition, want %s unchanged", j.State, Pending)
	}
}
