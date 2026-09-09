// Package rpc serves agents over one long-lived bidirectional gRPC stream:
// the server pushes assignments down it and the agent pushes heartbeats,
// progress, and results back up.
//
// It owns no storage. Dispatch leases jobs and records their outcomes with
// its own lease fencing; this package routes a leased job to a connected
// agent and reports back what that agent said. The registry is the whole of
// its state — who is connected, what they can run, and how to tell one of
// them that it is finished.
package rpc

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/job"
)

// Agent holds what the server needs to push to one stream. Every field is
// immutable after Register except capabilities and closed, both guarded by
// Registry.mu — which is why capabilities is unexported: List hands out these
// pointers, and an exported mutable map would be a race waiting for a reader.
type Agent struct {
	ID           string                 // immutable: the agent_id from its Register frame
	capabilities map[string]struct{}    // empty means "any type"; guarded by Registry.mu
	send         chan *pb.ServerToAgent // size 1, drop-on-full — see hub pattern
	cancel       context.CancelFunc     // ends this agent's Assign — per stream, never shared
	done         chan struct{}          // closed when this agent is gone; wakes every waiter at once
	closed       bool                   // guarded by Registry.mu; makes closing idempotent
}

// Registry is the set of currently connected agents, keyed by the agent_id
// each stream announced. A mutex and a map is the whole design: assignment is
// a walk over a handful of entries, so a manager goroutine would buy a
// scheduling hop and hide exactly the races -race finds for free.
//
// Every mutable field of every Agent is guarded by mu, which makes this type
// the sole synchronization authority for the pointers it hands out.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

// NewRegistry returns an empty registry, ready for concurrent use.
func NewRegistry() *Registry {
	return &Registry{agents: make(map[string]*Agent)}
}

// Register records the stream that has just identified itself as id and
// returns the Agent that Assign will deliver to. cancel must end that stream:
// it is how displacement and CloseAll stop an agent that is no longer reading.
//
// A second stream claiming id displaces the first, and the loser is closed
// here and now rather than left to discover its own eviction — anything
// waiting on it wakes immediately instead of waiting out a TCP timeout.
//
// Call this at most once per stream. cancel is per-stream, so a second call
// from one handler would fire that handler's own cancel as the displaced
// agent's and disconnect the caller. A re-Register on a live stream is a
// capability update: use SetCapabilities.
func (r *Registry) Register(id string, caps []string, send chan *pb.ServerToAgent, cancel context.CancelFunc) *Agent {
	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.agents[id]; ok {
		r.close(old)
	}

	capabilities := make(map[string]struct{})
	for _, c := range caps {
		capabilities[c] = struct{}{}
	}
	agent := &Agent{ID: id, capabilities: capabilities, send: send, cancel: cancel, done: make(chan struct{})}

	r.agents[id] = agent

	return agent
}

// SetCapabilities replaces the types a has announced, in place. Mutating
// keeps the *Agent identity, so no waiter is woken and no in-flight
// assignment is orphaned merely because an agent learned a new type.
func (r *Registry) SetCapabilities(a *Agent, caps []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	capabilities := make(map[string]struct{})
	for _, c := range caps {
		capabilities[c] = struct{}{}
	}
	a.capabilities = capabilities
}

// Unregister closes a and drops it; this is what a stream handler defers.
//
// It always closes, because waiters on a displaced agent must wake even
// though the map stopped pointing at it. It deletes only while the map still
// does point at a, so a handler unwinding after its displacement cannot evict
// the stream that replaced it.
func (r *Registry) Unregister(a *Agent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.close(a)
	if r.agents[a.ID] == a {
		delete(r.agents, a.ID)
	}
}

// List returns a snapshot of the connected agents. The slice is a copy; the
// Agents are not, so callers may compare them and wait on Done, but must not
// mutate them.
func (r *Registry) List() []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := []*Agent(nil)
	for _, a := range r.agents {
		list = append(list, a)
	}
	return list
}

// Assign hands j to one connected agent and returns the agent that took it,
// so the caller can wait on that stream in particular rather than on "some
// agent". It never blocks: each agent buffers a single frame, and one that
// has not drained its last assignment is passed over, not waited for.
//
// Selection is first capable idle agent wins, in map order; an agent that
// announced no capabilities accepts any type. Both failures are retryable,
// but they are distinguished because they ask the operator for different
// things: ErrSlowAgent means every capable agent is busy, so add agents,
// while ErrNoAgents means nothing connected runs this type at all.
//
// Agents already closed are skipped. CloseAll closes without deleting, so
// during shutdown the map still holds streams that are on their way out, and
// handing one a job would spend an attempt on a frame nobody will read.
func (r *Registry) Assign(j job.Job) (*Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	slow := false
	msg := &pb.ServerToAgent{Payload: &pb.ServerToAgent_Assignment{Assignment: &pb.Assignment{JobId: j.ID, Type: j.Type, Payload: j.Payload, Attempt: int32(j.Attempt)}}}
	for _, a := range r.agents {
		if a.closed {
			continue
		}
		if _, ok := a.capabilities[j.Type]; len(a.capabilities) == 0 || ok {
			select {
			case a.send <- msg:
				return a, nil
			default:
				slow = true
			}
		}
	}
	if slow {
		return nil, fmt.Errorf("%w: all agents with capability %q are full", ErrSlowAgent, j.Type)
	}
	return nil, fmt.Errorf("%w: can't take job's type %q", ErrNoAgents, j.Type)
}

// Cancel asks a to stop running jobID, and reports whether the frame was
// queued. Best effort by design: the send buffer holds one frame, and an agent
// that has not drained its last one is already behind, so the caller logs and
// moves on rather than blocking a dispatcher on a slow stream. The lease
// expiring is the real deadline, as the proto says.
//
// It lives here rather than on Agent because send and closed are registry-
// guarded state, and this type is their only synchronization authority.
func (r *Registry) Cancel(a *Agent, jobID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if a.closed {
		return false // the stream is already ending; nothing will read this
	}
	select {
	case a.send <- &pb.ServerToAgent{Payload: &pb.ServerToAgent_CancelJob{CancelJob: &pb.CancelJob{JobId: jobID}}}:
		return true
	default:
		return false
	}
}

// CloseAll ends every connected stream without emptying the map: each handler
// deletes its own entry as it unwinds, so map mutation keeps exactly one
// owner per agent. Shutdown calls this before grpc.Server.GracefulStop,
// which would otherwise wait forever — a bidi stream ends only when its
// context dies.
func (r *Registry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.agents {
		r.close(a)
	}
}

// Done is closed once this agent is gone: disconnected, displaced, or shut
// down. It is the only thing that releases a caller waiting for a result the
// agent will now never send. The lease cannot do that job, because the
// dispatcher renews leases on a ticker that knows nothing about whether the
// agent holding the work is still alive.
func (a *Agent) Done() <-chan struct{} {
	return a.done
}

// close ends a exactly once. The caller must hold mu: closed is
// registry-owned state, which is why this lives here and not on Agent, where
// it would either depend invisibly on another type's mutex or need a second
// synchronization mechanism of its own.
//
// Idempotence is required rather than defensive. Displacement closes the
// loser immediately and that loser's handler still calls Unregister on its
// way out; a second close of done panics.
//
// cancel runs under mu, so the woken handler blocks in its own deferred
// Unregister until the lock is released. That is brief and correct, but it is
// why no caller may hold mu for long around this.
func (r *Registry) close(a *Agent) {
	if a.closed {
		return
	}
	a.closed = true
	close(a.done) // broadcast: every waiter on this agent wakes
	a.cancel()    // the stream's Recv/Send return, the handler unwinds
}
