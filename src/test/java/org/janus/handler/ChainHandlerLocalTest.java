package org.janus.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.janus.observability.TracingHelper;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the WS JSON entry path with no downstream configured, so the handler
 * processes locally. Verifies the correlation id is echoed back (required for the
 * multiplexed WS client to match replies) and the greeting is produced.
 */
class ChainHandlerLocalTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChainHandler newHandler() {
        // noop OpenTelemetry keeps tracing inert; grpcClient=null and no wsClient
        // means route() falls through to local processing.
        return new ChainHandler(new TracingHelper(OpenTelemetry.noop()), null);
    }

    @Test
    void echoesRequestIdAndProcessesLocally() throws Exception {
        ChainHandler handler = newHandler();
        String request = "{\"method\":\"TALK\",\"mode\":\"REQUEST\",\"data\":\"0\",\"meta\":\"unit\",\"request_id\":\"corr-xyz\"}";

        String responseJson = handler.handleJsonRequest(request, new HashMap<>());
        JsonNode node = MAPPER.readTree(responseJson);

        assertEquals("corr-xyz", node.path("request_id").asText(), "correlation id must be echoed");
        assertEquals("RESPONSE", node.path("mode").asText());
        assertEquals(200, node.path("status").asInt());
        String data = node.path("results").get(0).path("kv").path("data").asText();
        assertTrue(data.startsWith("Hello"), "data 0 should greet with Hello, got: " + data);
    }

    @Test
    void unparseableRequestYieldsErrorEnvelope() throws Exception {
        ChainHandler handler = newHandler();
        String responseJson = handler.handleJsonRequest("{not json", new HashMap<>());
        JsonNode node = MAPPER.readTree(responseJson);
        assertEquals("ERROR", node.path("mode").asText());
    }

    /**
     * The WS server is an entry point — it should never see a RESPONSE/ERROR
     * envelope over the wire (downstream replies flow back through the
     * multiplexed forwarding client, not via a new server-side request).
     * Receiving one was previously misrouted as a fresh request, which would
     * either loop the chain or silently swallow the real reply. Now it must
     * come back as a hard 400-style ERROR envelope.
     */
    @Test
    void responseModeJsonEnvelopeIsRejected() throws Exception {
        ChainHandler handler = newHandler();
        String inbound = "{\"method\":\"TALK\",\"mode\":\"RESPONSE\",\"data\":\"0\",\"meta\":\"u\",\"request_id\":\"upstream-reply\"}";
        JsonNode node = MAPPER.readTree(handler.handleJsonRequest(inbound, new HashMap<>()));

        assertEquals("ERROR", node.path("mode").asText(), "non-REQUEST JSON must be refused");
        assertEquals("upstream-reply", node.path("request_id").asText(),
                "correlation id must be preserved so the upstream can match");
        assertTrue(node.path("error_msg").asText().contains("non-request"),
                "error_msg should name the cause; got: " + node.path("error_msg").asText());
    }

    @Test
    void errorModeJsonEnvelopeIsRejected() throws Exception {
        ChainHandler handler = newHandler();
        String inbound = "{\"method\":\"TALK\",\"mode\":\"ERROR\",\"error_msg\":\"downstream blew up\"}";
        JsonNode node = MAPPER.readTree(handler.handleJsonRequest(inbound, new HashMap<>()));

        assertEquals("ERROR", node.path("mode").asText());
        assertTrue(node.path("error_msg").asText().contains("non-request"));
    }
}
