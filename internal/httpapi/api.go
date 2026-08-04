package httpapi

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/mddemin/conveyor/internal/job"
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

// toResponse converts a domain job to its wire form. Every handler goes
// through here, so the JSON contract changes in exactly one place.
func toResponse(j *job.Job) jobResponse {
	return jobResponse{
		ID:        j.ID,
		Type:      j.Type,
		Payload:   json.RawMessage(j.Payload),
		State:     j.State.String(),
		Attempt:   j.Attempt,
		CreatedAt: j.CreatedAt,
		UpdatedAt: j.UpdatedAt,
	}
}

// idPattern is what a client-supplied id may contain. It becomes a URL path
// segment in Location and in GET /jobs/{id}: a slash would route the client
// to a job that does not exist, and a space would need escaping everywhere.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

const (
	maxTypeLen = 128
	maxIDLen   = 64
)

// validate reports whether the request can become a job. It is the only
// gate between a client's bytes and the domain.
func (r *enqueueRequest) validate() error {
	r.Type = strings.TrimSpace(r.Type)
	r.ID = strings.TrimSpace(r.ID)

	if r.Type == "" {
		return fmt.Errorf("%w: type is missing", errInvalidRequest)
	}

	if len(r.Type) > maxTypeLen {
		return fmt.Errorf("%w: type must be at most %d bytes, got %d",
			errInvalidRequest, maxTypeLen, len(r.Type))
	}

	if strings.ContainsFunc(r.Type, unicode.IsControl) {
		return fmt.Errorf("%w: type must not contain control characters", errInvalidRequest)
	}

	if r.ID != "" {
		if len(r.ID) > maxIDLen {
			return fmt.Errorf("%w: id must be at most %d bytes, got %d",
				errInvalidRequest, maxIDLen, len(r.ID))
		}
		if !idPattern.MatchString(r.ID) {
			return fmt.Errorf("%w: id may contain only letters, digits, '-', '_' and '.'",
				errInvalidRequest)
		}
	}

	if r.ID == "." || r.ID == ".." {
		return fmt.Errorf("%w: id must not be %q", errInvalidRequest, r.ID)
	}

	return nil
}
