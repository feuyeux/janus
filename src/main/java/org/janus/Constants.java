package org.janus;

import io.grpc.Context;
import io.grpc.Metadata;

import java.util.ArrayList;
import java.util.List;

public class Constants {

    public static final String SERVICE_NAME = "janus";
    public static final String GRPC_SERVICE = "janus.JanusService";

    // gRPC metadata keys for tracing
    public static final Metadata.Key<String> X_REQUEST_ID =
            Metadata.Key.of("x-request-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_TRACE_ID =
            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_SPAN_ID =
            Metadata.Key.of("x-span-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_B3_TRACEID =
            Metadata.Key.of("x-b3-traceid", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_B3_SPANID =
            Metadata.Key.of("x-b3-spanid", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_B3_PARENTSPANID =
            Metadata.Key.of("x-b3-parentspanid", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> X_B3_SAMPLED =
            Metadata.Key.of("x-b3-sampled", Metadata.ASCII_STRING_MARSHALLER);

    // Context keys
    public static final Context.Key<String> CTX_REQUEST_ID = Context.key("x-request-id");
    public static final Context.Key<String> CTX_TRACE_ID = Context.key("x-trace-id");
    public static final Context.Key<String> CTX_SPAN_ID = Context.key("x-span-id");

    // WebSocket trace propagation headers
    public static final String WS_HEADER_TRACE_ID = "X-Trace-Id";
    public static final String WS_HEADER_SPAN_ID = "X-Span-Id";
    public static final String WS_HEADER_REQUEST_ID = "X-Request-Id";

    // Discovery service names
    public static final String LB_ROUND_ROBIN = "round_robin";

    public static final List<Metadata.Key<String>> TRACING_KEYS;
    public static final List<Context.Key<String>> CONTEXT_KEYS;

    static {
        TRACING_KEYS = new ArrayList<>();
        TRACING_KEYS.add(X_REQUEST_ID);
        TRACING_KEYS.add(X_TRACE_ID);
        TRACING_KEYS.add(X_SPAN_ID);
        TRACING_KEYS.add(X_B3_TRACEID);
        TRACING_KEYS.add(X_B3_SPANID);
        TRACING_KEYS.add(X_B3_PARENTSPANID);
        TRACING_KEYS.add(X_B3_SAMPLED);

        CONTEXT_KEYS = new ArrayList<>();
        CONTEXT_KEYS.add(CTX_REQUEST_ID);
        CONTEXT_KEYS.add(CTX_TRACE_ID);
        CONTEXT_KEYS.add(CTX_SPAN_ID);
    }

    private Constants() {}
}
