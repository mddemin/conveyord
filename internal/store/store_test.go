package store

import (
	"errors"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func TestEnqueueStoresAPendingJob(t *testing.T) {
	s := freshDB(t)
	before := time.Now()

	j := &job.Job{ID: "j1", Type: "resize", Payload: []byte(`{"w":100}`)}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.State != job.Pending {
		t.Errorf("State = %s, want %s", j.State, job.Pending)
	}
	if j.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0", j.Attempt)
	}
	if j.CreatedAt.Before(before) {
		t.Errorf("CreatedAt = %v, not filled in from the database", j.CreatedAt)
	}
	if !j.UpdatedAt.Equal(j.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want it to equal CreatedAt = %v", j.UpdatedAt, j.CreatedAt)
	}
}

func TestEnqueueIgnoresCallerSuppliedState(t *testing.T) {
	s := freshDB(t)

	j := &job.Job{ID: "j1", Type: "resize", State: job.Dead, Attempt: 99}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.State != job.Pending || j.Attempt != 0 {
		t.Errorf("stored job is {%s, %d}, want {%s, 0}", j.State, j.Attempt, job.Pending)
	}
}

func TestEnqueueRejectsDuplicateID(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	if err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}

	err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "other"})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second Enqueue with the same ID returned %v, want ErrDuplicate", err)
	}
}
