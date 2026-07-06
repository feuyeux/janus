package org.janus.handler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.common.HelloUtils;
import org.janus.codec.BinaryCodec;
import org.janus.config.ServerConfig;
import org.janus.grpc.JanusGrpcClient;
import org.janus.model.JanusMessage;
import org.janus.observability.OtelSupport;
import org.janus.observability.TracingHelper;
import org.janus.proto.*;

import java.util.*;

/**
 * Handles request chaining with unified protocol translation.
 *
 * Receives a JanusEnvelope on any transport (WS JSON, WS Binary, gRPC),
 * and forwards to downstream on any other transport, or processes locally.
 *
 * Protocol translation matrix:
 *   WS JSON ←→ WS Binary ←→ gRPC
 */
public class ChainHandler {
    private static final Logger log = LoggerFactory.getLogger(ChainHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            // Downstream error envelopes / future schema drift must not blow up a hop.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final TracingHelper tracingHelper;
    private final JanusGrpcClient grpcClient;
    private org.janus.ws.JanusWsClient wsClient;

    public ChainHandler(TracingHelper tracingHelper, JanusGrpcClient grpcClient) {
        this.tracingHelper = tracingHelper;
        this.grpcClient = grpcClient;
    }

    public void setWsClient(org.janus.ws.JanusWsClient wsClient) {
        this.wsClient = wsClient;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WS JSON entry point
    // ═══════════════════════════════════════════════════════════════════════

    public String handleJsonRequest(String jsonRequest, Map<String, String> traceContext) {
        JanusMessage request;
        try {
            request = objectMapper.readValue(jsonRequest, JanusMessage.class);
        } catch (Exception e) {
            // Unparseable frame — a client-side fault, not a server error. Log at
            // WARN with just the message (no stack trace): the exception carries no
            // actionable server-side diagnostics, and dumping a full trace only
            // pollutes the logs for every malformed frame.
            log.warn("Discarding unparseable JSON request: {}", e.getMessage());
            return safeErrorJson(null, e);
        }

        OtelSupport.recordWsMessage("json-" + request.method());

        // WS text frames can't attach per-message headers, so rebuild the parent
        // context from the envelope's own trace ids when the carrier is empty.
        // This keeps WS→WS hops on the same trace.
        Map<String, String> ctx = mergeTraceContext(traceContext, request);
        Span span = tracingHelper.startServerSpan("ws-json-" + request.method(), ctx);
        try {
            JanusMessage response = route(request, ctx, span);
            // Echo the correlation id so the multiplexed WS client can match the reply.
            response = response.withRequestId(request.requestId());
            OtelSupport.recordWsMessage("json-response");
            return objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            log.error("Error handling JSON request", e);
            return safeErrorJson(request, e);
        } finally {
            tracingHelper.endSpan(span);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WS Binary entry point (MSG_JANUS unified message)
    // ═══════════════════════════════════════════════════════════════════════

    public byte[] handleBinaryJanus(BinaryCodec.JanusFrame frame) {
        Map<String, String> traceContext = new HashMap<>();
        if (frame.traceId() != null && !frame.traceId().isEmpty()) {
            traceContext.put("traceparent", "00-" + frame.traceId() + "-" + frame.spanId() + "-01");
        }

        String methodName = JanusMessage.methodFromIndex(frame.method());
        Span span = tracingHelper.startServerSpan("ws-binary-" + methodName, traceContext);
        OtelSupport.recordWsMessage("binary-" + methodName);

        try {
            // Convert binary frame to unified message
            JanusMessage request = new JanusMessage(
                    methodName,
                    frame.mode() == 0 ? JanusMessage.MODE_REQUEST : JanusMessage.MODE_RESPONSE,
                    frame.data(), frame.meta(),
                    null, null, null,
                    frame.traceId(), frame.spanId(),
                    frame.seq(), frame.streamEnd(), null);

            JanusMessage response = route(request, traceContext, span);

            // Convert response back to binary frame
            return envelopeToBinary(response);
        } catch (Exception e) {
            log.error("Error handling binary janus request", e);
            return BinaryCodec.encodeJanus(
                    frame.method(), 2, 0, true,
                    500, "", "", "", "", e.getMessage(), null);
        } finally {
            tracingHelper.endSpan(span);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WS Binary legacy entry point (ECHO_REQUEST)
    // ═══════════════════════════════════════════════════════════════════════

    public byte[] handleBinaryEcho(long echoId, String meta, String data, String traceId, String spanId) {
        Map<String, String> traceContext = new HashMap<>();
        if (traceId != null && !traceId.isEmpty()) {
            traceContext.put("traceparent", "00-" + traceId + "-" + spanId + "-01");
        }

        Span span = tracingHelper.startServerSpan("ws-binary-echo", traceContext);
        OtelSupport.recordWsMessage("binary-echo");

        try {
            JanusMessage request = JanusMessage.request(
                    JanusMessage.METHOD_TALK, data, meta, traceId, spanId, 0, true);
            JanusMessage response = route(request, traceContext, span);
            return envelopeToBinary(response);
        } finally {
            tracingHelper.endSpan(span);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Routing: choose transport based on config
    // ═══════════════════════════════════════════════════════════════════════

    private JanusMessage route(JanusMessage request, Map<String, String> traceContext, Span parentSpan) {
        String method = request.method() != null ? request.method() : JanusMessage.METHOD_TALK;
        // A node configured to forward must NOT silently answer locally when its
        // downstream is unavailable: doing so masks the outage and returns a
        // fabricated local result to the caller. Surface a 503 error envelope
        // instead so the failure propagates back up the chain. Local processing
        // is reserved for terminal nodes (no downstream configured).
        if (ServerConfig.isGrpcDownstream()) {
            if (grpcClient != null && grpcClient.isConnected()) {
                return forwardViaGrpc(request, traceContext, parentSpan);
            }
            log.warn("gRPC downstream configured but unavailable [{}]; returning 503", method);
            return JanusMessage.error(method, 503, "downstream gRPC unavailable");
        }
        if (ServerConfig.isWsDownstream()) {
            if (wsClient != null && wsClient.isConnected()) {
                return forwardViaWs(request, traceContext, parentSpan);
            }
            log.warn("WS downstream configured but unavailable [{}]; returning 503", method);
            return JanusMessage.error(method, 503, "downstream WS unavailable");
        }
        return processLocally(request);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Forward via gRPC (WS → gRPC protocol translation)
    // ═══════════════════════════════════════════════════════════════════════

    private JanusMessage forwardViaGrpc(JanusMessage request, Map<String, String> traceContext, Span parentSpan) {
        Span span = tracingHelper.startClientSpan("grpc-forward-" + request.method(), traceContext);
        try {
            String traceId = span.getSpanContext().getTraceId();
            String spanId = span.getSpanContext().getSpanId();

            TalkRequest grpcRequest = TalkRequest.newBuilder()
                    .setData(request.data() != null ? request.data() : "0")
                    .setMeta(request.meta() != null ? request.meta() : "JAVA")
                    .setTraceId(traceId)
                    .setSpanId(spanId)
                    .setSeq(request.seq() != null ? request.seq() : 0)
                    .setStreamEnd(request.streamEnd() != null ? request.streamEnd() : true)
                    .build();

            log.debug("Forwarding via gRPC [{}]: data={}, meta={}", request.method(), grpcRequest.getData(), grpcRequest.getMeta());

            return switch (request.method() != null ? request.method() : JanusMessage.METHOD_TALK) {
                case JanusMessage.METHOD_TALK_ONE_ANSWER_MORE -> {
                    // Server streaming: collect all responses
                    List<JanusMessage.JanusResult> allResults = new ArrayList<>();
                    int seq = 0;
                    Iterator<TalkResponse> it = grpcClient.getBlockingStub().talkOneAnswerMore(grpcRequest);
                    while (it.hasNext()) {
                        TalkResponse resp = it.next();
                        allResults.addAll(talkResultsToEnvelope(resp.getResultsList()));
                        seq = resp.getSeq();
                    }
                    yield JanusMessage.response(request.method(), 200, allResults, traceId, spanId, seq, true);
                }
                case JanusMessage.METHOD_TALK_MORE_ANSWER_ONE -> {
                    // Client streaming is collapsed to a single unary call here:
                    // the WS→gRPC bridge receives one logical message at a time, so
                    // it cannot replay a true client stream. The downstream still
                    // returns one aggregated response. Full client-streaming
                    // fan-in is only available on the native gRPC entry path.
                    TalkResponse resp = grpcClient.getBlockingStub().talk(grpcRequest);
                    yield grpcResponseToEnvelope(request.method(), resp, traceId, spanId);
                }
                case JanusMessage.METHOD_TALK_BIDIRECTIONAL -> {
                    // Bidirectional streaming is likewise collapsed to unary for the
                    // WS→gRPC bridge (one request in, one response out). Native
                    // bidi semantics are preserved only on the gRPC entry path.
                    TalkResponse resp = grpcClient.getBlockingStub().talk(grpcRequest);
                    yield grpcResponseToEnvelope(request.method(), resp, traceId, spanId);
                }
                default -> {
                    // Unary
                    TalkResponse resp = grpcClient.getBlockingStub().talk(grpcRequest);
                    yield grpcResponseToEnvelope(JanusMessage.METHOD_TALK, resp, traceId, spanId);
                }
            };
        } finally {
            tracingHelper.endSpan(span);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Forward via WebSocket (WS → WS, protocol passes through)
    // ═══════════════════════════════════════════════════════════════════════

    private JanusMessage forwardViaWs(JanusMessage request, Map<String, String> traceContext, Span parentSpan) {
        Span span = tracingHelper.startClientSpan("ws-forward-" + request.method(), traceContext);
        try {
            String traceId = span.getSpanContext().getTraceId();
            String spanId = span.getSpanContext().getSpanId();
            String correlationId = UUID.randomUUID().toString();
            // Carry the client-span ids in the envelope so the downstream WS node
            // rebuilds this span as its parent (WS text frames can't carry
            // headers), plus a fresh correlation id so the multiplexed pool can
            // match the reply to this in-flight request.
            JanusMessage outbound = request.withTrace(traceId, spanId).withRequestId(correlationId);
            String jsonRequest = objectMapper.writeValueAsString(outbound);
            log.debug("Forwarding via WS [{}] corr={}: data={}, meta={}",
                    request.method(), correlationId, request.data(), request.meta());
            String jsonResponse = wsClient.forward(correlationId, jsonRequest);
            return objectMapper.readValue(jsonResponse, JanusMessage.class);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("JSON serialization error during WS forward", e);
        } finally {
            tracingHelper.endSpan(span);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Local processing (terminal node)
    // ═══════════════════════════════════════════════════════════════════════

    public JanusMessage processLocally(JanusMessage request) {
        String method = request.method() != null ? request.method() : JanusMessage.METHOD_TALK;
        String data = request.data() != null ? request.data() : "0";
        String meta = request.meta() != null ? request.meta() : "JAVA";

        log.debug("Processing locally [{}]: data={}, meta={}", method, data, meta);

        return switch (method) {
            case JanusMessage.METHOD_TALK_ONE_ANSWER_MORE -> {
                // Server streaming: split data by comma, return multiple results
                String[] items = data.split(",");
                List<JanusMessage.JanusResult> results = new ArrayList<>();
                for (String item : items) {
                    results.add(createResult(item, meta));
                }
                yield JanusMessage.response(method, 200, results);
            }
            case JanusMessage.METHOD_TALK_MORE_ANSWER_ONE -> {
                // Client streaming: single request, return single result
                yield JanusMessage.response(method, 200, List.of(createResult(data, meta)));
            }
            case JanusMessage.METHOD_TALK_BIDIRECTIONAL -> {
                // Bidi: single request, single response
                yield JanusMessage.response(method, 200, List.of(createResult(data, meta)));
            }
            default -> {
                // Unary
                yield JanusMessage.response(JanusMessage.METHOD_TALK, 200, List.of(createResult(data, meta)));
            }
        };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Protocol conversion helpers
    // ═══════════════════════════════════════════════════════════════════════

    private JanusMessage grpcResponseToEnvelope(String method, TalkResponse resp, String traceId, String spanId) {
        List<JanusMessage.JanusResult> results = talkResultsToEnvelope(resp.getResultsList());
        return JanusMessage.response(method, resp.getStatus(), results,
                traceId, spanId, resp.getSeq(), resp.getStreamEnd());
    }

    private List<JanusMessage.JanusResult> talkResultsToEnvelope(List<TalkResult> grpcResults) {
        List<JanusMessage.JanusResult> results = new ArrayList<>();
        for (TalkResult r : grpcResults) {
            results.add(new JanusMessage.JanusResult(
                    r.getId(),
                    r.getType() == ResultType.OK ? "OK" : "FAIL",
                    new HashMap<>(r.getKvMap())));
        }
        return results;
    }

    private byte[] envelopeToBinary(JanusMessage msg) {
        int methodIdx = msg.methodIndex();
        int modeIdx = switch (msg.mode() != null ? msg.mode() : JanusMessage.MODE_RESPONSE) {
            case JanusMessage.MODE_REQUEST -> 0;
            case JanusMessage.MODE_ERROR -> 2;
            default -> 1;
        };
        BinaryCodec.EchoResult[] results = null;
        if (msg.results() != null && !msg.results().isEmpty()) {
            results = new BinaryCodec.EchoResult[msg.results().size()];
            for (int i = 0; i < msg.results().size(); i++) {
                JanusMessage.JanusResult r = msg.results().get(i);
                results[i] = new BinaryCodec.EchoResult(
                        r.id(),
                        "FAIL".equals(r.type()) ? 1 : 0,
                        r.kv() != null ? r.kv() : Map.of());
            }
        }
        return BinaryCodec.encodeJanus(
                methodIdx, modeIdx,
                msg.seq() != null ? msg.seq() : 0,
                msg.streamEnd() != null ? msg.streamEnd() : true,
                msg.status() != null ? msg.status() : 0,
                msg.data(), msg.meta(),
                msg.traceId(), msg.spanId(), msg.errorMsg(),
                results);
    }

    private JanusMessage.JanusResult createResult(String data, String meta) {
        String greeting = HelloUtils.getGreeting(data);
        String answer = HelloUtils.getAnswer(greeting);
        return new JanusMessage.JanusResult(
                System.nanoTime(), "OK", Map.of(
                        "id", UUID.randomUUID().toString(),
                        "idx", data,
                        "data", greeting + "," + answer,
                        "meta", meta));
    }

    /**
     * Build the parent trace context for an inbound JSON request. Prefers any
     * context supplied by the transport; otherwise reconstructs a {@code
     * traceparent} from the envelope's own trace/span ids so WS→WS hops stay on
     * one trace.
     */
    private static Map<String, String> mergeTraceContext(Map<String, String> base, JanusMessage req) {
        Map<String, String> ctx = new HashMap<>();
        if (base != null) {
            ctx.putAll(base);
        }
        if (!ctx.containsKey("traceparent")
                && req.traceId() != null && !req.traceId().isEmpty()) {
            String spanId = (req.spanId() != null && !req.spanId().isEmpty())
                    ? req.spanId() : "0000000000000000";
            ctx.put("traceparent", "00-" + req.traceId() + "-" + spanId + "-01");
        }
        return ctx;
    }

    /**
     * Serialize a proper error envelope, preserving the inbound correlation id so
     * the upstream multiplexed client can complete (rather than time out) the
     * matching request.
     */
    private String safeErrorJson(JanusMessage request, Exception e) {
        String method = request != null && request.method() != null
                ? request.method() : JanusMessage.METHOD_TALK;
        String corrId = request != null ? request.requestId() : null;
        try {
            JanusMessage err = JanusMessage.error(method, e.getMessage()).withRequestId(corrId);
            return objectMapper.writeValueAsString(err);
        } catch (Exception ex) {
            // Last resort: hand-built JSON with escaping.
            StringBuilder sb = new StringBuilder("{\"mode\":\"ERROR\",\"error_msg\":\"")
                    .append(jsonEscape(e.getMessage())).append("\"");
            if (corrId != null) {
                sb.append(",\"request_id\":\"").append(jsonEscape(corrId)).append("\"");
            }
            return sb.append("}").toString();
        }
    }

    /**
     * Minimal JSON string escaping for the last-resort error fallback, used only
     * when the ObjectMapper itself failed. Ensures the emitted body stays valid
     * JSON even if the message contains quotes, backslashes or control chars.
     */
    static String jsonEscape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
