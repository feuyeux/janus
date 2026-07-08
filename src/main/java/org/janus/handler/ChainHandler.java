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

        // The WS server is an ENTRY point. A WS text frame arriving with
        // mode=RESPONSE/ERROR is not a valid request — downstream replies flow
        // back through the multiplexed forwarding client's request_id, not via a
        // new server-side request. Refuse it as a 400 (protocol error) instead
        // of routing it downstream, which would either loop the chain or confuse
        // the next hop. Null/missing mode is treated as REQUEST (legacy clients).
        if (request.mode() != null && !request.isRequest()) {
            log.warn("Rejecting JSON envelope with non-REQUEST mode={} corr={}; expected inbound REQUEST only",
                    request.mode(), request.requestId());
            OtelSupport.recordWsMessage("json-bad-mode");
            return safeErrorJson(request, new IllegalArgumentException(
                    "non-request frame rejected: mode=" + request.mode()));
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
        // A WS server only ever receives REQUEST frames over the wire: downstream
        // REPLY/ERROR frames are matched by the multiplexed forwarding client via
        // request_id, never by re-entering the server's MSG_JANUS handler. A
        // non-REQUEST frame here is therefore a protocol error (e.g. an upstream
        // peer bounced a reply back into the entry point) — refuse it loudly
        // instead of silently routing it downstream, which would either loop the
        // chain or confuse the next hop.
        if (frame.mode() != 0) {
            log.warn("Rejecting MSG_JANUS frame with non-REQUEST mode={} corr={}; expected inbound REQUEST only",
                    frame.mode(), frame.requestId());
            OtelSupport.recordWsMessage("binary-bad-mode");
            return BinaryCodec.encodeJanus(
                    frame.method(), 2, 0, true,
                    400, "", "", "", "", "non-request frame rejected", frame.requestId(), null);
        }
        Span span = tracingHelper.startServerSpan("ws-binary-" + methodName, traceContext);
        OtelSupport.recordWsMessage("binary-" + methodName);

        try {
            // Convert binary frame to unified message, preserving the per-hop
            // correlation id so a multiplexed binary connection can match replies.
            JanusMessage request = new JanusMessage(
                    methodName,
                    JanusMessage.MODE_REQUEST,
                    frame.data(), frame.meta(),
                    null, null, null,
                    frame.traceId(), frame.spanId(),
                    frame.seq(), frame.streamEnd(), frame.requestId());

            JanusMessage response = route(request, traceContext, span);

            // Echo back the correlation id we received so the upstream multiplexed
            // binary client can match this reply to its in-flight request.
            response = response.withRequestId(frame.requestId());

            // Convert response back to binary frame
            return envelopeToBinary(response);
        } catch (Exception e) {
            log.error("Error handling binary janus request", e);
            return BinaryCodec.encodeJanus(
                    frame.method(), 2, 0, true,
                    500, "", "", "", "", e.getMessage(), frame.requestId(), null);
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
    // gRPC entry point (native gRPC in → route to WS downstream or local)
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Handle a request received on the native gRPC server and route it through
     * the unified chain. This is what lets a middle node <em>receive via gRPC and
     * forward via WebSocket</em> (S2 in the S1[WS→gRPC] → S2[gRPC→WS] → S3[WS→local]
     * topology): the inbound {@link TalkRequest} is lifted into the unified
     * envelope, routed (WS forward or local processing), then lowered back to a
     * {@link TalkResponse}.
     *
     * <p>gRPC carries the method via the RPC name (not a field), so the caller
     * passes it in explicitly.
     */
    public TalkResponse handleGrpcRequest(String method, TalkRequest request) {
        Map<String, String> traceContext = new HashMap<>();
        if (request.getTraceId() != null && !request.getTraceId().isEmpty()) {
            String spanId = (request.getSpanId() != null && !request.getSpanId().isEmpty())
                    ? request.getSpanId() : "0000000000000000";
            traceContext.put("traceparent", "00-" + request.getTraceId() + "-" + spanId + "-01");
        }

        Span span = tracingHelper.startServerSpan("grpc-" + method, traceContext);
        OtelSupport.recordRpcCall(method);
        try {
            JanusMessage envelope = JanusMessage.request(
                    method, request.getData(), request.getMeta(),
                    request.getTraceId(), request.getSpanId(),
                    request.getSeq(), request.getStreamEnd());
            JanusMessage response = route(envelope, traceContext, span);
            return envelopeToTalkResponse(response);
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
            // rebuilds this span as its parent (WS frames can't carry headers),
            // plus a fresh correlation id so the multiplexed pool can match the
            // reply to this in-flight request.
            JanusMessage outbound = request.withTrace(traceId, spanId).withRequestId(correlationId);

            if (wsClient.isBinary()) {
                // Encode as a MSG_JANUS binary frame (the downstream, e.g. S3,
                // only speaks binary). The requestId is carried in-frame so the
                // multiplexed binary connection can correlate the reply.
                byte[] reqBytes = envelopeToBinary(outbound);
                log.debug("Forwarding via WS Binary [{}] corr={}: data={}, meta={}",
                        request.method(), correlationId, request.data(), request.meta());
                byte[] respBytes = wsClient.forwardBinary(correlationId, reqBytes);
                return binaryFrameToEnvelope(BinaryCodec.decodeJanus(respBytes));
            }

            String jsonRequest = objectMapper.writeValueAsString(outbound);
            log.debug("Forwarding via WS JSON [{}] corr={}: data={}, meta={}",
                    request.method(), correlationId, request.data(), request.meta());
            String jsonResponse = wsClient.forward(correlationId, jsonRequest);
            return objectMapper.readValue(jsonResponse, JanusMessage.class);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("JSON serialization error during WS forward", e);
        } catch (BinaryCodec.DecodeException e) {
            throw new RuntimeException("Binary decode error during WS forward", e);
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

    /**
     * Lower a unified response envelope back into a gRPC {@link TalkResponse}.
     * Used by the native gRPC entry path when a middle node receives via gRPC and
     * forwards via WebSocket (the WS round-trip returns an envelope, which is then
     * handed back to the gRPC caller).
     */
    private TalkResponse envelopeToTalkResponse(JanusMessage msg) {
        TalkResponse.Builder builder = TalkResponse.newBuilder()
                .setStatus(msg.status() != null ? msg.status() : 200)
                .setSeq(msg.seq() != null ? msg.seq() : 0)
                .setStreamEnd(msg.streamEnd() != null ? msg.streamEnd() : true);
        if (msg.results() != null) {
            for (JanusMessage.JanusResult r : msg.results()) {
                builder.addResults(TalkResult.newBuilder()
                        .setId(r.id())
                        .setType("FAIL".equals(r.type()) ? ResultType.FAIL : ResultType.OK)
                        .putAllKv(r.kv() != null ? r.kv() : Map.of())
                        .build());
            }
        }
        return builder.build();
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
                msg.requestId(), results);
    }

    /**
     * Lift a decoded MSG_JANUS binary frame back into the unified envelope. Used
     * when a binary WS forwarding reply comes back from the downstream (S3), so
     * the middle node can hand a normal {@link JanusMessage} to the rest of the
     * chain.
     */
    static JanusMessage binaryFrameToEnvelope(BinaryCodec.JanusFrame frame) {
        String method = JanusMessage.methodFromIndex(frame.method());
        String mode = switch (frame.mode()) {
            case 0 -> JanusMessage.MODE_REQUEST;
            case 2 -> JanusMessage.MODE_ERROR;
            default -> JanusMessage.MODE_RESPONSE;
        };
        List<JanusMessage.JanusResult> results = new ArrayList<>();
        if (frame.results() != null) {
            for (BinaryCodec.EchoResult r : frame.results()) {
                results.add(new JanusMessage.JanusResult(
                        r.idx(), r.type() == 1 ? "FAIL" : "OK",
                        r.kv() != null ? r.kv() : Map.of()));
            }
        }
        String errorMsg = (frame.errorMsg() != null && !frame.errorMsg().isEmpty()) ? frame.errorMsg() : null;
        return new JanusMessage(method, mode, frame.data(), frame.meta(),
                frame.status(), results.isEmpty() ? null : results, errorMsg,
                frame.traceId(), frame.spanId(), frame.seq(), frame.streamEnd(), frame.requestId());
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
