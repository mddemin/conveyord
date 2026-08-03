package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// decode returns the error alone; every case below only cares about that.
func decode(t *testing.T, body string) error {
	t.Helper()
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
	_, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	return err
}

// Every rejection must carry errInvalidRequest, because that sentinel is the
// only thing standing between a malformed body and a 500.
func TestDecodeJSONRejections(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string // a fragment the client-facing message must contain
	}{
		{"unknown field", `{"type":"resize","nope":1}`, "nope"},
		{"trailing garbage", `{"type":"resize"}{"type":"other"}`, "single JSON value"},
		{"malformed", `{"type":`, ""},
		{"empty body", ``, "empty"},
		{"wrong type", `{"type":123}`, "type"},
		// Valid JSON, just too much of it. A body that is *syntactically* bad
		// fails in the parser long before the limit and never reaches
		// MaxBytesReader at all.
		{"oversized", `{"type":"` + strings.Repeat("a", maxBodyBytes) + `"}`, "exceed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := decode(t, tt.body)
			if err == nil {
				t.Fatal("decodeJSON accepted it")
			}
			if !errors.Is(err, errInvalidRequest) {
				t.Errorf("error is %v, want it to wrap errInvalidRequest", err)
			}
			if tt.want != "" && !strings.Contains(err.Error(), tt.want) {
				t.Errorf("message is %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

// The size limit belongs in the message. A client told "too large" without a
// number cannot act on it.
func TestDecodeJSONSaysHowLargeIsTooLarge(t *testing.T) {
	err := decode(t, `{"type":"`+strings.Repeat("a", maxBodyBytes)+`"}`)
	if err == nil {
		t.Fatal("decodeJSON accepted a body over the limit")
	}
	if !strings.Contains(err.Error(), strconv.Itoa(maxBodyBytes)) {
		t.Errorf("message is %q, want it to name the %d byte limit", err, maxBodyBytes)
	}
}

func TestDecodeJSONPassesPayloadThroughVerbatim(t *testing.T) {
	const payload = `{"url":"https://example.com","depth":3}`
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(
		`{"type":"crawl","payload":`+payload+`}`))

	got, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	if err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
	// Not base64, not re-serialized with different key order — the same bytes.
	if string(got.Payload) != payload {
		t.Errorf("payload round-tripped as %s, want %s", got.Payload, payload)
	}
}

func TestWriteJSONSetsContentTypeAndStatus(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusCreated, jobResponse{ID: "j1", State: "pending"})

	if w.Code != http.StatusCreated {
		t.Errorf("status is %d, want %d", w.Code, http.StatusCreated)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type is %q, want application/json", ct)
	}
	var got jobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response body is not JSON: %v", err)
	}
	if got.ID != "j1" {
		t.Errorf("body has id %q, want j1", got.ID)
	}
}
