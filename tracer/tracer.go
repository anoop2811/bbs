package tracer

import (
	"code.cloudfoundry.org/lager"
	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"

	"os"
)

const (
	// Our service name.
	serviceName = "bbs"

	// Host + port of our service.
	hostAndPort = "127.0.0.1:61002"

	// Endpoint to send Zipkin spans to.
	zipkinHTTPEndpoint = "http://10.80.139.54:9411/api/v1/spans"

	// Debug mode.
	isDebug = true

	// same span can be set to true for RPC style spans (Zipkin V1) vs Node style (OpenTracing)
	isSameSpan = true

	// make Tracer generate 128 bit traceID's for root spans.
	isTraceID128Bit = true
)

func NewTracer(logger lager.Logger) opentracing.Tracer {
	logger.Info("initializing-zipkin-tracer")
	collector, err := zipkin.NewHTTPCollector(zipkinHTTPEndpoint)
	if err != nil {
		logger.Error("unable to create Zipkin HTTP collector: %+v", err)
		os.Exit(-1)
	}
	recorder := zipkin.NewRecorder(collector, isDebug, hostAndPort, serviceName)

	// Create our HTTP Service.
	tracer, err := zipkin.NewTracer(
		recorder,
		zipkin.ClientServerSameSpan(isSameSpan),
		zipkin.TraceID128Bit(isTraceID128Bit),
	)
	opentracing.InitGlobalTracer(tracer)

	logger.Info("initialized-zipkin-tracer")
	return tracer
}
