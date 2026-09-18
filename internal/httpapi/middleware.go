package httpapi

import (
	"context"
	"crypto/rand"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
	"unicode"
)

func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

type ctxKey int

const (
	requestIDKey    ctxKey = 0
	maxRequestIDLen int    = 64
)

// logger returns the request-scoped logger: a.log with the request ID attached
// if withRequestID ran, a.log otherwise.
func (a *API) logger(ctx context.Context) *slog.Logger {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return a.log.With("request_id", id)
	}
	return a.log
}

type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (rec *recorder) Write(b []byte) (int, error) {
	rec.wroteHeader = true
	n, err := rec.ResponseWriter.Write(b)
	rec.bytes += n
	return n, err
}

func (rec *recorder) WriteHeader(code int) {
	rec.status = code
	rec.wroteHeader = true
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *recorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

func (a *API) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > maxRequestIDLen || strings.ContainsFunc(id, unicode.IsControl) {
			id = rand.Text()[:16]
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *API) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		defer func() {
			a.logger(r.Context()).InfoContext(r.Context(), "request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				// Named for its unit. A bare time.Duration serializes as an
				// integer count of nanoseconds, which reads like milliseconds.
				"dur_ms", float64(time.Since(start).Microseconds())/1000)
		}()
		next.ServeHTTP(rec, r)
	})
}

func (a *API) withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v) // not mine to handle
				}
				a.logger(r.Context()).ErrorContext(r.Context(), "panic recovered",
					"err", v,
					"stack", string(debug.Stack()),
					"method", r.Method,
					"path", r.URL.Path,
				)

				if rec, ok := w.(*recorder); !ok || !rec.wroteHeader {
					// The only thing that can fail here is the write, the client is already gone,
					// and the panic is logged above
					_ = writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (a *API) withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.TimeoutHandler(next, d, `{"error":"timeout"}`)
}
