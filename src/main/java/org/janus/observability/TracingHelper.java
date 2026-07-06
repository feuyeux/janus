package org.janus.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;

public class TracingHelper {
    private static final Logger log = LoggerFactory.getLogger(TracingHelper.class);

    private final Tracer tracer;

    public TracingHelper(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("janus-server-java");
    }

    /**
     * Start a server span from propagated trace context in a Map carrier.
     *
     * <p>After starting, the new server span is <em>injected back</em> into the
     * carrier. This is what makes the chain nest correctly: any client span
     * subsequently started from the same carrier (e.g. a downstream gRPC/WS
     * forward) becomes a child of this server span rather than a sibling — and
     * on an entry node with no inbound context it prevents the downstream call
     * from starting a second, disconnected trace.
     */
    public Span startServerSpan(String spanName, Map<String, String> traceContext) {
        Context parentContext = OtelSupport.getOpenTelemetry().getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), traceContext, OtelSupport.mapGetter());

        Span span = tracer.spanBuilder(spanName)
                .setParent(parentContext)
                .setSpanKind(SpanKind.SERVER)
                .startSpan();

        // Overwrite the carrier so the started server span is the parent for any
        // downstream span or forwarded envelope built from this same context.
        Context withSpan = span.storeInContext(parentContext);
        OtelSupport.getOpenTelemetry().getPropagators()
                .getTextMapPropagator()
                .inject(withSpan, traceContext, OtelSupport.mapSetter());

        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        MDC.put("traceId", traceId);
        MDC.put("spanId", spanId);
        log.debug("Span started: {} traceId={} spanId={}", spanName, traceId, spanId);
        return span;
    }

    /**
     * Start a client span and inject trace context into a Map carrier for propagation.
     */
    public Span startClientSpan(String spanName, Map<String, String> traceContext) {
        Context parentContext = OtelSupport.getOpenTelemetry().getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), traceContext, OtelSupport.mapGetter());

        Span span = tracer.spanBuilder(spanName)
                .setParent(parentContext)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        Context withSpan = span.storeInContext(parentContext);
        OtelSupport.getOpenTelemetry().getPropagators()
                .getTextMapPropagator()
                .inject(withSpan, traceContext, OtelSupport.mapSetter());

        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        log.debug("Client span started: {} traceId={} spanId={}", spanName, traceId, spanId);
        return span;
    }

    /**
     * End a span and clean up MDC.
     */
    public void endSpan(Span span) {
        if (span != null) {
            span.end();
            MDC.remove("traceId");
            MDC.remove("spanId");
        }
    }

    /**
     * End a span with error.
     */
    public void endSpanWithError(Span span, Throwable throwable) {
        if (span != null) {
            span.recordException(throwable);
            span.setAttribute("error", true);
            span.end();
            MDC.remove("traceId");
            MDC.remove("spanId");
        }
    }
}
