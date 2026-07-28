package job

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanent(t *testing.T) {
	base := errors.New("bad request")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", base, false},
		{"marked", Permanent(base), true},
		{"marked then wrapped", fmt.Errorf("calling api: %w", Permanent(base)), true},
		{"wrapped then marked", Permanent(fmt.Errorf("calling api: %w", base)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Errorf("IsPermanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPermanentPreservesTheUnderlyingError(t *testing.T) {
	base := errors.New("bad request")
	err := fmt.Errorf("calling api: %w", Permanent(base))

	if !errors.Is(err, base) {
		t.Errorf("errors.Is(err, base) = false, want true — marking must not hide the cause")
	}
	if got, want := Permanent(base).Error(), "bad request"; got != want {
		t.Errorf("Permanent(base).Error() = %q, want %q", got, want)
	}
}

func TestPermanentOfNilIsNil(t *testing.T) {
	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}
}
