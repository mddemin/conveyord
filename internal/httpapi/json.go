package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const maxBodyBytes = 1024 * 1024

// decodeJSON reads the request body as a single JSON value of type T. It caps
// the body at maxBodyBytes, rejects unknown fields, and rejects anything
// trailing the first value.
//
// Every error describing a bad body wraps errInvalidRequest and is safe to show
// the client. An error that does not wrap it is a transport failure, not the
// client's fault, and must not be echoed back.
func decodeJSON[T any](w http.ResponseWriter, r *http.Request) (T, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var v T
	if err := dec.Decode(&v); err != nil {
		var synErr *json.SyntaxError
		if errors.As(err, &synErr) {
			return v, fmt.Errorf("%w: malformed JSON, offset: %v", errInvalidRequest, synErr.Offset)
		}
		var unmErr *json.UnmarshalTypeError
		if errors.As(err, &unmErr) {
			if unmErr.Field == "" {
				return v, fmt.Errorf("%w: body must be a JSON object, got %s", errInvalidRequest, unmErr.Value)
			}
			return v, fmt.Errorf("%w: field %q must be %s, got %s", errInvalidRequest, unmErr.Field, unmErr.Type, unmErr.Value)
		}
		if errors.Is(err, io.EOF) {
			return v, fmt.Errorf("%w: body must not be empty", errInvalidRequest)
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return v, fmt.Errorf("%w: body contains malformed JSON", errInvalidRequest)
		}
		var maxBErr *http.MaxBytesError
		if errors.As(err, &maxBErr) {
			return v, fmt.Errorf("%w: body must not exceed %v bytes", errInvalidRequest, maxBErr.Limit)
		}
		if strings.HasPrefix(err.Error(), "json: unknown field ") {
			return v, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
		}
		return v, err
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return v, fmt.Errorf("%w: body must contain a single JSON value", errInvalidRequest)
	}
	return v, nil
}

// writeJSON marshals v and sends it with status. It buffers first so a marshal
// failure becomes a clean 500 rather than a truncated 200.
//
// A non-nil error means the response could not be delivered as asked:
// either v was unmarshalable (a 500 was sent instead) or the connection failed mid-write.
// Either way it is for the log — nothing remains to be written.
func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	b, err := json.Marshal(v)
	if err != nil {

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
		return fmt.Errorf("marshaling %T: %w", v, err)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, err = w.Write(b)
	return err
}
