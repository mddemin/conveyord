// Package obs is the OpenTelemetry seam: trace context as a W3C traceparent
// string, slog attrs from the span in context, a few instruments, and pprof.
//
// internal/job does not import this. The carrier travels as Job.Traceparent,
// a string the domain never interprets — the same way it never interprets
// CreatedAt.
package obs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentation = "github.com/mddemin/conveyor"

// Instruments are no-ops until Setup or a test provider is installed.
var (
	JobDuration  metric.Float64Histogram
	LeaseExpired metric.Int64Counter
	QueueDepth   metric.Int64Gauge
)

func init() {
	mustInstruments()
}

func mustInstruments() {
	m := otel.Meter(instrumentation)
	var err error
	JobDuration, err = m.Float64Histogram("conveyor.job.duration", metric.WithUnit("s"))
	if err != nil {
		panic(err)
	}
	LeaseExpired, err = m.Int64Counter("conveyor.lease.expired")
	if err != nil {
		panic(err)
	}
	QueueDepth, err = m.Int64Gauge("conveyor.queue.depth")
	if err != nil {
		panic(err)
	}
}

// Setup installs a TracerProvider for this process. An OTLP HTTP exporter is
// added only when OTEL_EXPORTER_OTLP_ENDPOINT is set — Jaeger/Compose is
// phase 12; until then slog carrying trace_id is the operator's view.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", service),
	))
	if err != nil {
		return nil, err
	}

	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	mustInstruments()
	return tp.Shutdown, nil
}

// Traceparent is the W3C header for ctx's span, or "" if there isn't one.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}

// Continue returns ctx with the remote span from a stored traceparent as
// parent. Empty input is a no-op, so a job enqueued without a tracer still
// runs.
func Continue(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// Start is Tracer().Start with job attributes.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(instrumentation).Start(ctx, name, trace.WithAttributes(attrs...))
}

func JobAttrs(id, typ string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("job.id", id),
		attribute.String("job.type", typ),
	}
}

// Handler copies trace_id and span_id from ctx onto every record. Log calls
// must use the *Context methods or Handle sees context.Background() and
// these attrs stay absent.
type Handler struct {
	slog.Handler
}

func (h Handler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return Handler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h Handler) WithGroup(name string) slog.Handler {
	return Handler{Handler: h.Handler.WithGroup(name)}
}

// ServePprof binds addr until ctx dies. Empty addr is a no-op so the flag
// defaults off.
func ServePprof(ctx context.Context, addr string) error {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()

	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
