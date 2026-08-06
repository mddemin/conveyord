package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/pgtest"
	"github.com/mddemin/conveyor/internal/store"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func freshDB(t *testing.T) *store.Store {
	t.Helper()
	pool := pgtest.Pool(t)

	ctx := t.Context()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store.New(pool)
}

// The subset of the wire format this test asserts on. Deliberately a separate
// declaration from httpapi's: sharing the server's own struct would make the
// test agree with the server by construction, including when both are wrong.
type jobResponse struct {
	ID      string          `json:"id"`
	State   string          `json:"state"`
	Payload json.RawMessage `json:"payload"`
}

func TestEnqueueAndReadBackOverHTTP(t *testing.T) {
	s := freshDB(t) // the real *store.Store
	api := httpapi.New(s, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ts := httptest.NewServer(api)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	body := strings.NewReader(`{"type":"resize","payload":{"w":100}}`)
	req, err := http.NewRequestWithContext(ctx, "POST", ts.URL+"/jobs", body)
	if err != nil {
		t.Fatalf("building the POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /jobs returned %d, want 201", resp.StatusCode)
	}

	var created jobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decoding the created job: %v", err)
	}
	if created.State != "pending" {
		t.Errorf("created job is %s, want pending", created.State)
	}

	// Follow Location rather than rebuilding the URL. A 201 promises the new
	// resource is fetchable there; constructing "/jobs/"+id by hand tests the
	// test's idea of the route instead of the server's.
	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("201 carried no Location header")
	}

	// Read it back through the API, not through the store — the point is the
	// round trip, including the payload surviving as JSON rather than base64.
	req, err = http.NewRequestWithContext(ctx, "GET", ts.URL+loc, nil)
	if err != nil {
		t.Fatalf("building the GET for %q: %v", loc, err)
	}
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", loc, err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d, want 200", loc, resp2.StatusCode)
	}

	var got jobResponse
	if err := json.NewDecoder(resp2.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the fetched job: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("read back id %s, want %s", got.ID, created.ID)
	}
	if string(got.Payload) != `{"w":100}` {
		t.Errorf("payload round-tripped as %s, want the bytes the client sent", got.Payload)
	}
}
