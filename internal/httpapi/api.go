package httpapi

import (
	"encoding/json"
	"time"
)

// enqueueRequest is the body of POST /jobs.
type enqueueRequest struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// jobResponse is a job as clients see it. It is deliberately not job.Job:
// the domain type is free to change without breaking the API.
type jobResponse struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	State     string          `json:"state"`
	Attempt   int             `json:"attempt"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// errorResponse is the body of every 4xx and 5xx.
type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}
