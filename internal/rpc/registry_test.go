package rpc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mddemin/conveyor/internal/job"
	"golang.org/x/sync/errgroup"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
)

func TestRegistryIsConcurrentSafe(t *testing.T) {
	r := NewRegistry()
	var g errgroup.Group
	for i := range 50 {
		id := fmt.Sprintf("a-%d", i)
		g.Go(func() error {
			a := r.Register(id, nil, make(chan *pb.ServerToAgent, 1), func() {})
			r.Unregister(a)
			return nil
		})
		g.Go(func() error { _, _ = r.Assign(job.Job{ID: id, Type: "x"}); return nil })
		g.Go(func() error { r.CloseAll(); return nil })
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	// -race covers the map; this covers the bookkeeping: every Register above is
	// paired with an Unregister, so a leaked entry is a lost cancel func.
	if n := len(r.List()); n != 0 {
		t.Fatalf("registry not drained: %d agents left", n)
	}
}

func TestDuplicateIDDisplacesOldStream(t *testing.T) {
	r := NewRegistry()
	cancelled := make(chan struct{})
	old := r.Register("a-1", nil, make(chan *pb.ServerToAgent, 1), func() { close(cancelled) })
	fresh := r.Register("a-1", nil, make(chan *pb.ServerToAgent, 1), func() {})
	<-cancelled       // displacement must end the old stream, or it leaks until TCP notices
	<-old.Done()      // and must wake anyone waiting on the loser, immediately
	r.Unregister(old) // the displaced handler unwinds late: must not evict fresh, must not double-close
	if got := r.List(); len(got) != 1 || got[0] != fresh {
		t.Fatalf("displaced agent evicted its replacement: %v", got)
	}
}

func TestUnregisterWakesWaiters(t *testing.T) {
	r := NewRegistry()
	a := r.Register("a-1", nil, make(chan *pb.ServerToAgent, 1), func() {})
	select {
	case <-a.Done():
		t.Fatal("Done closed while the agent is still registered")
	default:
	}
	r.Unregister(a)
	<-a.Done() // a blocked Run must have exactly this wake-up; hangs here on failure
}

func TestCloseAllWakesWaiters(t *testing.T) {
	r := NewRegistry()
	a := r.Register("a-1", nil, make(chan *pb.ServerToAgent, 1), func() {})
	r.CloseAll()
	<-a.Done()
	if n := len(r.List()); n != 1 {
		t.Fatalf("CloseAll must not delete; handlers do: %d", n)
	}
	// Still in the map, and that is the point: an agent whose stream is going
	// away must not be handed work during the drain.
	if _, err := r.Assign(job.Job{ID: "j1", Type: "x"}); !errors.Is(err, ErrNoAgents) {
		t.Fatalf("closed agent must not be assignable: %v", err)
	}
	r.Unregister(a) // idempotent close: this must not panic
}

func TestAssignDropsForSlowAgent(t *testing.T) {
	r := NewRegistry()
	ch := make(chan *pb.ServerToAgent, 1)
	ch <- &pb.ServerToAgent{Payload: &pb.ServerToAgent_Assignment{Assignment: &pb.Assignment{JobId: "blocked"}}}
	r.Register("slow", nil, ch, func() {})
	_, err := r.Assign(job.Job{ID: "j2", Type: "x"})
	if !errors.Is(err, ErrSlowAgent) {
		t.Fatalf("want ErrSlowAgent, got %v", err)
	}
}

func TestAssignSkipsFullAgent(t *testing.T) {
	r := NewRegistry()
	full := make(chan *pb.ServerToAgent, 1)
	full <- &pb.ServerToAgent{}
	free := make(chan *pb.ServerToAgent, 1)
	r.Register("full", nil, full, func() {})
	fresh := r.Register("free", nil, free, func() {})
	// Map order is random: this fails whenever Assign returns on the first full
	// agent instead of walking past it.
	got, err := r.Assign(job.Job{ID: "j1", Type: "x"})
	if err != nil {
		t.Fatalf("free agent should receive, got %v", err)
	}
	if got != fresh {
		t.Fatalf("Assign returned the wrong agent: %v", got)
	}
	if len(free) != 1 {
		t.Fatal("assignment did not reach the free agent")
	}
}

func TestCapabilityFiltering(t *testing.T) {
	r := NewRegistry()
	r.Register("img", []string{"resize"}, make(chan *pb.ServerToAgent, 1), func() {})
	if _, err := r.Assign(job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("capable agent should receive, got %v", err)
	}
	if _, err := r.Assign(job.Job{ID: "j2", Type: "transcode"}); !errors.Is(err, ErrNoAgents) {
		t.Fatalf("want ErrNoAgents, got %v", err)
	}
}

func TestSetCapabilitiesKeepsTheSameAgent(t *testing.T) {
	r := NewRegistry()
	a := r.Register("img", []string{"resize"}, make(chan *pb.ServerToAgent, 1), func() {
		t.Fatal("a capability update must never cancel the stream")
	})

	r.SetCapabilities(a, []string{"resize", "transcode"})

	select {
	case <-a.Done():
		t.Fatal("a capability update must not wake waiters")
	default:
	}
	if got := r.List(); len(got) != 1 || got[0] != a {
		t.Fatalf("SetCapabilities replaced the agent: %v", got)
	}
	if _, err := r.Assign(job.Job{ID: "j1", Type: "transcode"}); err != nil {
		t.Fatalf("newly announced type should be assignable, got %v", err)
	}
}
