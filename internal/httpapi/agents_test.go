package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"testing"

	"github.com/mddemin/conveyor/internal/hub"
)

// "Is my agent connected?" was previously answerable only by reading the
// daemon's log, which made a job sitting pending indistinguishable from a job
// no connected agent can run.
func TestListAgentsReportsTheFleet(t *testing.T) {
	api := New(stubStore{}, stubAgents{
		{ID: "a-1", Capabilities: []string{"resize", "sleep"}},
		{ID: "a-2", Capabilities: []string{}},
	}, hub.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	w := do(api, "GET", "/agents", "")

	if w.Code != http.StatusOK {
		t.Fatalf("status is %d, want 200: %s", w.Code, w.Body)
	}
	var got []AgentInfo
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding %s: %v", w.Body, err)
	}
	want := []AgentInfo{
		{ID: "a-1", Capabilities: []string{"resize", "sleep"}},
		{ID: "a-2", Capabilities: []string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GET /agents returned %+v, want %+v", got, want)
	}
}

// An empty fleet is a fact worth reporting precisely: [] parses as an empty
// list in every client, null needs a nil check nobody writes.
func TestListAgentsReturnsAnEmptyArrayNotNull(t *testing.T) {
	api := New(stubStore{}, stubAgents(nil), hub.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	w := do(api, "GET", "/agents", "")

	if got := w.Body.String(); got != "[]" {
		t.Errorf("body is %q, want an empty JSON array", got)
	}
}

// A daemon wired without a registry must not claim the fleet is empty — that
// is a fact an operator acts on. No route is the honest answer.
func TestAgentsIsUnroutedWithoutARegistry(t *testing.T) {
	api := New(stubStore{}, nil, hub.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	if w := do(api, "GET", "/agents", ""); w.Code != http.StatusNotFound {
		t.Errorf("status is %d, want 404 when no registry is wired", w.Code)
	}
}
