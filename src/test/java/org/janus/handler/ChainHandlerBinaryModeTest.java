package org.janus.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.janus.codec.BinaryCodec;
import org.janus.observability.TracingHelper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the MSG_JANUS binary path of the chain handler.
 *
 * <p>A WS server is an entry point. The multiplexed forwarding client
 * correlates downstream replies back to callers via the per-hop
 * {@code request_id}, so the server-side MSG_JANUS handler should never
 * receive a RESPONSE or ERROR frame over the wire. Before the fix, such
 * frames were misrouted as fresh REQUESTs (mode 1/2 → MODE_RESPONSE in the
 * unified envelope, which {@link ChainHandler#route} then sent downstream,
 * causing a loop or a swallowed reply).
 */
class ChainHandlerBinaryModeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChainHandler newHandler() {
        return new ChainHandler(new TracingHelper(OpenTelemetry.noop()), null);
    }

    @Test
    void requestBinaryFrameIsProcessedLocally() throws Exception {
        // Build a valid MSG_JANUS REQUEST frame (mode=0, method=TALK(0)).
        byte[] frame = BinaryCodec.encodeJanus(
                0, 0, 0, true, 0, "0", "u", "tid", "sid", "", "corr-1", null);

        byte[] reply = newHandler().handleBinaryJanus(BinaryCodec.decodeJanus(frame));
        BinaryCodec.JanusFrame out = BinaryCodec.decodeJanus(reply);

        assertEquals(0, out.method());
        // mode 1 = RESPONSE
        assertEquals(1, out.mode(), "successful local processing must emit a RESPONSE frame");
        assertEquals("corr-1", out.requestId(), "correlation id must be echoed");
        assertEquals(200, out.status());
    }

    @Test
    void responseBinaryFrameIsRejected() throws Exception {
        // Downstream reply (mode=1) that somehow ended up in the server-side
        // MSG_JANUS handler — previously misrouted as a fresh request.
        byte[] frame = BinaryCodec.encodeJanus(
                0, 1, 0, true, 200, "stale", "u", "tid", "sid", "", "stale-corr", null);

        byte[] reply = newHandler().handleBinaryJanus(BinaryCodec.decodeJanus(frame));
        BinaryCodec.JanusFrame out = BinaryCodec.decodeJanus(reply);

        // mode 2 = ERROR, status 400
        assertEquals(2, out.mode(), "non-REQUEST frame must be refused with an ERROR frame");
        assertEquals(400, out.status());
        assertEquals("stale-corr", out.requestId(), "correlation id must be preserved");
        assertTrue(out.errorMsg() != null && out.errorMsg().contains("non-request"),
                "errorMsg should name the cause; got: " + out.errorMsg());
    }

    @Test
    void errorBinaryFrameIsRejected() throws Exception {
        // Downstream ERROR (mode=2) that arrived at the entry point.
        byte[] frame = BinaryCodec.encodeJanus(
                0, 2, 0, true, 500, "", "u", "tid", "sid", "downstream blew up", "err-corr", null);

        byte[] reply = newHandler().handleBinaryJanus(BinaryCodec.decodeJanus(frame));
        BinaryCodec.JanusFrame out = BinaryCodec.decodeJanus(reply);

        assertEquals(2, out.mode(), "ERROR frame in must produce ERROR frame out");
        assertEquals(400, out.status(), "treated as a protocol error, not a forwarded 500");
        assertEquals("err-corr", out.requestId());
    }
}
