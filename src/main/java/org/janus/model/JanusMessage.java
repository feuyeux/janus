package org.janus.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Unified Janus message envelope.
 * Shared logical model across WS JSON, WS Binary, and gRPC transports.
 *
 * <p>{@code requestId} is a per-hop correlation id used by the multiplexed WS
 * forwarding client so that many concurrent in-flight requests can share a
 * single downstream connection: the downstream echoes back the same id and the
 * client completes the matching pending future. Each node stamps the response
 * with the request id it received, so correlation is leg-local and nests
 * correctly across S1→S2→S3.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JanusMessage(
        @JsonProperty("method") String method,
        @JsonProperty("mode") String mode,
        @JsonProperty("data") String data,
        @JsonProperty("meta") String meta,
        @JsonProperty("status") Integer status,
        @JsonProperty("results") List<JanusResult> results,
        @JsonProperty("error_msg") String errorMsg,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("span_id") String spanId,
        @JsonProperty("seq") Integer seq,
        @JsonProperty("stream_end") Boolean streamEnd,
        @JsonProperty("request_id") String requestId
) {
    // ── Method constants ─────────────────────────────────────────────────────
    public static final String METHOD_TALK = "TALK";
    public static final String METHOD_TALK_ONE_ANSWER_MORE = "TALK_ONE_ANSWER_MORE";
    public static final String METHOD_TALK_MORE_ANSWER_ONE = "TALK_MORE_ANSWER_ONE";
    public static final String METHOD_TALK_BIDIRECTIONAL = "TALK_BIDIRECTIONAL";

    // ── Mode constants ───────────────────────────────────────────────────────
    public static final String MODE_REQUEST = "REQUEST";
    public static final String MODE_RESPONSE = "RESPONSE";
    public static final String MODE_ERROR = "ERROR";

    // ── Factory: request ─────────────────────────────────────────────────────
    public static JanusMessage request(String method, String data, String meta) {
        return new JanusMessage(method, MODE_REQUEST, data, meta,
                null, null, null, null, null, 0, true, null);
    }

    public static JanusMessage request(String method, String data, String meta,
                                          String traceId, String spanId, int seq, boolean streamEnd) {
        return new JanusMessage(method, MODE_REQUEST, data, meta,
                null, null, null, traceId, spanId, seq, streamEnd, null);
    }

    // ── Factory: response ────────────────────────────────────────────────────
    public static JanusMessage response(String method, int status, List<JanusResult> results) {
        return new JanusMessage(method, MODE_RESPONSE, null, null,
                status, results, null, null, null, 0, true, null);
    }

    public static JanusMessage response(String method, int status, List<JanusResult> results,
                                           String traceId, String spanId, int seq, boolean streamEnd) {
        return new JanusMessage(method, MODE_RESPONSE, null, null,
                status, results, null, traceId, spanId, seq, streamEnd, null);
    }

    // ── Factory: error ───────────────────────────────────────────────────────
    public static JanusMessage error(String method, String errorMsg) {
        return new JanusMessage(method, MODE_ERROR, null, null,
                null, null, errorMsg, null, null, 0, true, null);
    }

    // ── Withers (records are immutable; rebuild with one field changed) ───────
    /** Return a copy carrying the given correlation id. */
    public JanusMessage withRequestId(String newRequestId) {
        return new JanusMessage(method, mode, data, meta, status, results, errorMsg,
                traceId, spanId, seq, streamEnd, newRequestId);
    }

    /** Return a copy carrying the given trace/span ids (used for WS→WS propagation). */
    public JanusMessage withTrace(String newTraceId, String newSpanId) {
        return new JanusMessage(method, mode, data, meta, status, results, errorMsg,
                newTraceId, newSpanId, seq, streamEnd, requestId);
    }

    // ── Convenience getters ──────────────────────────────────────────────────
    public boolean isRequest() {
        return MODE_REQUEST.equals(mode);
    }

    public boolean isResponse() {
        return MODE_RESPONSE.equals(mode);
    }

    public boolean isError() {
        return MODE_ERROR.equals(mode);
    }

    public int methodIndex() {
        return switch (method != null ? method : METHOD_TALK) {
            case METHOD_TALK_ONE_ANSWER_MORE -> 1;
            case METHOD_TALK_MORE_ANSWER_ONE -> 2;
            case METHOD_TALK_BIDIRECTIONAL -> 3;
            default -> 0;
        };
    }

    public static String methodFromIndex(int index) {
        return switch (index) {
            case 1 -> METHOD_TALK_ONE_ANSWER_MORE;
            case 2 -> METHOD_TALK_MORE_ANSWER_ONE;
            case 3 -> METHOD_TALK_BIDIRECTIONAL;
            default -> METHOD_TALK;
        };
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record JanusResult(
            @JsonProperty("id") long id,
            @JsonProperty("type") String type,
            @JsonProperty("kv") Map<String, String> kv
    ) {
        public static JanusResult ok(String data, String meta) {
            return new JanusResult(System.nanoTime(), "OK", Map.of(
                    "id", UUID.randomUUID().toString(),
                    "idx", data,
                    "data", data,
                    "meta", meta
            ));
        }
    }
}
