package org.janus.observability;

import io.grpc.ClientInterceptor;
import io.grpc.ServerInterceptor;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ServiceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.config.ServerConfig;

import java.util.List;
import java.util.Map;

public final class OtelSupport {
    private static final Logger log = LoggerFactory.getLogger(OtelSupport.class);

    private static OpenTelemetry openTelemetry;
    private static PrometheusHttpServer prometheusServer;
    private static LongCounter rpcCallsCounter;
    private static LongCounter wsMessagesCounter;

    private OtelSupport() {}

    public static boolean otelEnabled() {
        return ServerConfig.OTEL_ENABLED;
    }

    public static synchronized OpenTelemetry initOtel(String serviceName) {
        if (openTelemetry != null) {
            return openTelemetry;
        }
        if (!otelEnabled()) {
            openTelemetry = OpenTelemetry.noop();
            return openTelemetry;
        }

        log.info("Initializing OpenTelemetry: service={}, endpoint={}", serviceName, ServerConfig.OTEL_ENDPOINT);

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        ServiceAttributes.SERVICE_NAME, serviceName,
                        io.opentelemetry.api.common.AttributeKey.stringKey("service.instance.id"),
                        ServerConfig.SERVER_ID)));

        // Traces: OTLP gRPC exporter -> Jaeger/OTLP collector
        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(ServerConfig.OTEL_ENDPOINT)
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .build();

        // Metrics: Prometheus HTTP server (scraped by Prometheus)
        prometheusServer = PrometheusHttpServer.builder()
                .setHost("0.0.0.0")
                .setPort(ServerConfig.METRICS_PORT)
                .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(prometheusServer)
                .setResource(resource)
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .build();

        // Initialize counters
        Meter meter = openTelemetry.getMeter("janus-server-java");
        rpcCallsCounter = meter.counterBuilder("rpc_calls_total")
                .setDescription("Total number of RPC calls handled")
                .setUnit("{call}")
                .build();
        wsMessagesCounter = meter.counterBuilder("ws_messages_total")
                .setDescription("Total number of WebSocket messages handled")
                .setUnit("{message}")
                .build();

        log.info("OpenTelemetry initialized: metrics on port {}", ServerConfig.METRICS_PORT);
        return openTelemetry;
    }

    public static OpenTelemetry getOpenTelemetry() {
        if (openTelemetry == null) {
            return OpenTelemetry.noop();
        }
        return openTelemetry;
    }

    public static Tracer tracer() {
        return getOpenTelemetry().getTracer("janus-server-java");
    }

    public static LongCounter rpcCallsCounter() {
        return rpcCallsCounter;
    }

    public static LongCounter wsMessagesCounter() {
        return wsMessagesCounter;
    }

    public static void recordRpcCall(String method) {
        if (rpcCallsCounter != null) {
            rpcCallsCounter.add(1, Attributes.of(
                    io.opentelemetry.api.common.AttributeKey.stringKey("method"), method));
        }
    }

    public static void recordWsMessage(String type) {
        if (wsMessagesCounter != null) {
            wsMessagesCounter.add(1, Attributes.of(
                    io.opentelemetry.api.common.AttributeKey.stringKey("type"), type));
        }
    }

    public static ServerInterceptor grpcServerInterceptor() {
        if (!otelEnabled()) return null;
        GrpcTelemetry grpcTelemetry = GrpcTelemetry.builder(getOpenTelemetry()).build();
        return grpcTelemetry.createServerInterceptor();
    }

    public static ClientInterceptor grpcClientInterceptor() {
        if (!otelEnabled()) return null;
        GrpcTelemetry grpcTelemetry = GrpcTelemetry.builder(getOpenTelemetry()).build();
        return grpcTelemetry.createClientInterceptor();
    }

    public static void shutdown() {
        if (prometheusServer != null) {
            prometheusServer.shutdown();
        }
        if (openTelemetry instanceof OpenTelemetrySdk sdk) {
            sdk.getSdkTracerProvider().shutdown();
            sdk.getSdkMeterProvider().shutdown();
        }
    }

    // ─── Trace context propagation for WebSocket ──────────────────────────────

    public static final TextMapSetter<Map<String, String>> MAP_SETTER = (carrier, key, value) -> {
        if (carrier != null) carrier.put(key, value);
    };

    public static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier == null ? List.of() : carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    @SuppressWarnings("unchecked")
    public static <C> TextMapSetter<C> mapSetter() {
        return (TextMapSetter<C>) (Object) MAP_SETTER;
    }

    @SuppressWarnings("unchecked")
    public static <C> TextMapGetter<C> mapGetter() {
        return (TextMapGetter<C>) (Object) MAP_GETTER;
    }
}
