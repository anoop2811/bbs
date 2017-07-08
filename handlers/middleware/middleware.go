package middleware

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/runtimeschema/metric"
)

const (
	RequestLatency = metric.Duration("RequestLatency")
	RequestCount   = metric.Counter("RequestCount")
)

type LoggableHandlerFunc func(logger lager.Logger, w http.ResponseWriter, r *http.Request)

func LogWrap(logger, accessLogger lager.Logger, loggableHandlerFunc LoggableHandlerFunc) http.HandlerFunc {
	lagerDataFromReq := func(r *http.Request) lager.Data {
		return lager.Data{
			"method":  r.Method,
			"request": r.URL.String(),
		}
	}

	if accessLogger != nil {
		return func(w http.ResponseWriter, r *http.Request) {
			requestLog := logger.Session("request", lagerDataFromReq(r))
			requestAccessLogger := accessLogger.Session("request", lagerDataFromReq(r))

			requestAccessLogger.Info("serving")

			requestLog.Debug("serving")

			start := time.Now()
			defer requestLog.Debug("done")
			defer func() {
				requestAccessLogger.Info("done", lager.Data{"duration": time.Since(start)})
			}()
			loggableHandlerFunc(requestLog, w, r)
		}
	} else {
		return func(w http.ResponseWriter, r *http.Request) {
			requestLog := logger.Session("request", lagerDataFromReq(r))

			requestLog.Debug("serving")
			defer requestLog.Debug("done")

			loggableHandlerFunc(requestLog, w, r)

		}
	}
}

func FromHTTPRequest(tracer opentracing.Tracer, operationName string,
	traceableHandlerFunc http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Try to join to a trace propagated in `req`.
		wireContext, err := tracer.Extract(
			opentracing.TextMap,
			opentracing.HTTPHeadersCarrier(req.Header),
		)
		if err != nil {
			fmt.Printf("error encountered while trying to extract span: %+v\n", err)
		}

		// create span
		span := tracer.StartSpan(operationName, ext.RPCServerOption(wireContext))
		span.SetTag("serverSide", "here")
		defer span.Finish()

		// store span in context
		ctx := opentracing.ContextWithSpan(req.Context(), span)

		// update request context to include our new span
		req = req.WithContext(ctx)
		traceableHandlerFunc(w, req)
	})
}

type RequestFunc func(req *http.Request) *http.Request

func ToHTTPRequest(tracer opentracing.Tracer) RequestFunc {
	return func(req *http.Request) *http.Request {
		// Retrieve the Span from context.
		if span := opentracing.SpanFromContext(req.Context()); span != nil {

			// We are going to use this span in a client request, so mark as such.
			ext.SpanKindRPCClient.Set(span)

			// Add some standard OpenTracing tags, useful in an HTTP request.
			ext.HTTPMethod.Set(span, req.Method)
			span.SetTag(zipkincore.HTTP_HOST, req.URL.Host)
			span.SetTag(zipkincore.HTTP_PATH, req.URL.Path)
			ext.HTTPUrl.Set(
				span,
				fmt.Sprintf("%s://%s%s", req.URL.Scheme, req.URL.Host, req.URL.Path),
			)

			// Add information on the peer service we're about to contact.
			if host, portString, err := net.SplitHostPort(req.URL.Host); err == nil {
				ext.PeerHostname.Set(span, host)
				if port, err := strconv.Atoi(portString); err != nil {
					ext.PeerPort.Set(span, uint16(port))
				}
			} else {
				ext.PeerHostname.Set(span, req.URL.Host)
			}

			// Inject the Span context into the outgoing HTTP Request.
			if err := tracer.Inject(
				span.Context(),
				opentracing.TextMap,
				opentracing.HTTPHeadersCarrier(req.Header),
			); err != nil {
				fmt.Printf("error encountered while trying to inject span: %+v", err)
			}
		}
		return req
	}
}

func NewLatencyEmitterWrapper(emitter Emitter) LatencyEmitterWrapper {
	return LatencyEmitterWrapper{emitter: emitter}
}

type LatencyEmitterWrapper struct {
	emitter Emitter
}

func (l LatencyEmitterWrapper) RecordLatency(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		f(w, r)
		l.emitter.UpdateLatency(time.Since(startTime))
	}
}

//go:generate counterfeiter -o fakes/fake_emitter.go . Emitter
type Emitter interface {
	IncrementCounter(delta int)
	UpdateLatency(latency time.Duration)
}

type defaultEmitter struct {
}

func (e *defaultEmitter) IncrementCounter(delta int) {
	RequestCount.Increment()
}

func (e *defaultEmitter) UpdateLatency(latency time.Duration) {
}

func RequestCountWrap(handler http.Handler) http.HandlerFunc {
	return RequestCountWrapWithCustomEmitter(handler, &defaultEmitter{})
}

func RequestCountWrapWithCustomEmitter(handler http.Handler, emitter Emitter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		emitter.IncrementCounter(1)
		handler.ServeHTTP(w, r)
	}
}
