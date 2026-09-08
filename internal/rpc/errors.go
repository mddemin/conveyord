package rpc

import "errors"

// Registry.Assign returns the first two of these wrapped, Server.Run the
// third; match them with errors.Is. All three mean "try later" and none is
// job.Permanent, so Policy.OnFailure retries a job that hits any of them.
// They are three errors rather than one because they ask the operator for
// different things.
var (
	// ErrNoAgents means nothing currently connected declares this job's type:
	// either no agent is connected at all, or every one that is announced a
	// capability set without it. Retrying will keep failing until an agent
	// that runs the type connects, so this is a deployment or configuration
	// signal, not a load signal.
	ErrNoAgents = errors.New("no agents available")

	// ErrSlowAgent means capable agents are connected but every one of them
	// still holds an undelivered assignment. This is backpressure: the work
	// can run, there is just nowhere to put it this instant. Retrying is
	// expected to succeed, and a steady stream of these means add agents.
	ErrSlowAgent = errors.New("agent is slow")

	// ErrAgentGone means the assignment was delivered and the agent then
	// vanished without ever reporting. Unlike the other two the job may have
	// run, or may still be running, so the retry this triggers is the
	// at-least-once case an operator needs to be able to see in the log.
	ErrAgentGone = errors.New("agent disconnected before reporting")
)
