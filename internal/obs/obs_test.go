package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestTraceparentRoundTrip(t *testing.T) {
	_, restore := TestProvider()
	defer restore()

	ctx, span := Start(context.Background(), "root")
	defer span.End()
	tp := Traceparent(ctx)
	if tp == "" {
		t.Fatal("span in context produced an empty traceparent")
	}
	child := Continue(context.Background(), tp)
	if got, want := Traceparent(child), tp; got != want {
		t.Fatalf("Continue lost the carrier:\n got %q\nwant %q", got, want)
	}
}

func TestHandlerAddsTraceID(t *testing.T) {
	_, restore := TestProvider()
	defer restore()

	var buf bytes.Buffer
	log := slog.New(Handler{Handler: slog.NewJSONHandler(&buf, nil)})
	ctx, span := Start(context.Background(), "log")
	defer span.End()
	log.InfoContext(ctx, "hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["trace_id"] == nil || rec["span_id"] == nil {
		t.Fatalf("record missing trace attrs: %s", buf.Bytes())
	}
	if rec["msg"] != "hello" {
		t.Fatalf("msg=%v", rec["msg"])
	}
}

func TestHandlerSilentWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(Handler{Handler: slog.NewJSONHandler(&buf, nil)})
	log.InfoContext(context.Background(), "hello")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("no span should not invent a trace_id: %s", buf.Bytes())
	}
}
