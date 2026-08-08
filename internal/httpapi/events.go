package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// heartbeatDuration is how long a stream may stay silent. Short enough that
// proxies with the usual 60-second idle timeout do not reap a quiet stream;
// long enough that it is not traffic. API.heartbeat holds the live value so a
// test can shrink it — a heartbeat test that waits 15 seconds is one nobody
// runs.
const heartbeatDuration = 15 * time.Second

func (a *API) jobEvents(w http.ResponseWriter, r *http.Request) {
	jobFilter := r.PathValue("id")
	a.events(w, r, jobFilter)
}

func (a *API) allEvents(w http.ResponseWriter, r *http.Request) {
	a.events(w, r, "")
}

// events streams job events until the client disconnects. jobFilter selects
// one job; "" streams every job.
//
// No backlog is sent: a client that connects after the fact sees nothing that
// already happened, by design. Replaying history here would mean deciding how
// far back to go and reconciling it with live events, to reproduce something
// GET /jobs/{id} already answers exactly. The stream carries what is happening
// now; the store carries what happened.
func (a *API) events(w http.ResponseWriter, r *http.Request, jobFilter string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	// Ignored by net/http on HTTP/1.1 and dropped outright on HTTP/2, which
	// forbids connection-specific headers. Kept because every SSE client and
	// proxy expects to see it; nothing here depends on it.
	w.Header().Set("Connection", "keep-alive")
	// Not standard HTTP: it tells nginx not to buffer. Without it a proxy
	// re-introduces the exact buffering that flushing exists to prevent, and
	// only in deployment.
	w.Header().Set("X-Accel-Buffering", "no")

	// One call, two jobs: it sends the headers so the client knows the stream
	// is open, and it reports whether this response can be flushed at all.
	// A type assertion cannot answer the second question — any wrapper in the
	// chain that declares Flush would answer it for the writer underneath.
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		a.writeError(w, r, fmt.Errorf("stream not supported: %w", err))
		return
	}

	sub := a.hub.Subscribe(jobFilter)
	defer sub.Close()

	ticker := time.NewTicker(a.heartbeat)
	defer ticker.Stop() // the ticker's goroutine and timer live until you stop it

	for {
		select {
		case ev, ok := <-sub.C:
			if !ok {
				return // hub closed the subscription
			}
			// Events were dropped for this client. It cannot be told what it
			// missed — that is what dropping means — so it is told to refetch,
			// before the event it did get, so the frames read in causal order.
			// Lagged clears as it reads: one lag episode, one frame.
			if sub.Lagged() {
				if err := writeEvent(w, "lagged", struct{}{}); err != nil {
					return
				}
				_ = rc.Flush()
			}

			if err := writeEvent(w, "job", ev); err != nil {
				return // the client is gone; the deferred Close frees its slot
			}
			// Errors are ignored from here on: the first Flush already proved
			// this writer can flush, and a later failure shows up as a write
			// error on the next event anyway.
			_ = rc.Flush()

		case <-ticker.C:
			// A comment line, not an event — clients ignore it. It exists so
			// proxies do not reap an idle connection, and so a client can tell
			// "no events yet" from "the server died".
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return // writing to a dead connection is often how you find out
			}
			_ = rc.Flush()

		case <-r.Context().Done():
			return // client disconnected
		}
	}
}

// writeEvent writes one SSE frame. The trailing blank line is what dispatches
// the event — without it a client holds a complete event and never fires a
// listener. data is marshalled rather than formatted because a raw newline in
// a data: value would start a new field and corrupt the frame; JSON escapes
// them, so the framing survives a job with a multi-line error message.
func writeEvent(w io.Writer, name string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling %s event: %w", name, err)
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
	return err
}
