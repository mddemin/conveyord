package job

import "testing"

func TestStateString(t *testing.T) {
	if got := Pending.String(); got != "pending" {
		t.Errorf("Pending.String() = %q, want %q", got, "pending")
	}
}

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state State
		want  bool
	}{
		{Pending, false},
		{Leased, false},
		{Failed, false},
		{Succeeded, true},
		{Dead, true},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			if got := tt.state.Terminal(); got != tt.want {
				t.Errorf("%s.Terminal() = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}
