package org.janus;

import org.junit.jupiter.api.Test;
import org.janus.discovery.ServiceRegistry.ServiceInstance;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JanusServer#parseStaticDownstreams(String, String, int, String)},
 * the pure parser behind {@code JANUS_DOWNSTREAM_HOSTS}/{@code JANUS_DOWNSTREAM_HOST}.
 */
class JanusServerStaticDownstreamTest {

    @Test
    void hostsCsvParsedWithExplicitPorts() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "a:1001,b:1002,c:1003", "", 8080, "ws");
        assertEquals(3, out.size());
        assertEquals(new ServiceInstance("a", 1001, "ws"), out.get(0));
        assertEquals(new ServiceInstance("b", 1002, "ws"), out.get(1));
        assertEquals(new ServiceInstance("c", 1003, "ws"), out.get(2));
    }

    @Test
    void omittedPortFallsBackToDefault() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "a,b:2000", "", 8080, "ws");
        assertEquals(2, out.size());
        assertEquals(8080, out.get(0).port(), "no port → default");
        assertEquals(2000, out.get(1).port(), "explicit port kept");
    }

    @Test
    void unparseablePortFallsBackToDefault() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "a:notaport", "", 8080, "ws");
        assertEquals(1, out.size());
        assertEquals(8080, out.get(0).port());
    }

    @Test
    void blankEntriesAndWhitespaceAreSkippedAndTrimmed() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                " a:1 , , b:2 ,", "", 8080, "ws");
        assertEquals(2, out.size());
        assertEquals(new ServiceInstance("a", 1, "ws"), out.get(0));
        assertEquals(new ServiceInstance("b", 2, "ws"), out.get(1));
    }

    @Test
    void hostsCsvTakesPrecedenceOverSingleHost() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "csv-host:1", "single-host", 8080, "ws");
        assertEquals(1, out.size());
        assertEquals("csv-host", out.get(0).host());
    }

    @Test
    void fallsBackToSingleHostWhenCsvBlank() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "", "single", 9090, "grpc");
        assertEquals(1, out.size());
        assertEquals(new ServiceInstance("single", 9090, "grpc"), out.get(0));
    }

    @Test
    void nullCsvTreatedAsBlank() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                null, "single", 9090, "grpc");
        assertEquals(1, out.size());
        assertEquals("single", out.get(0).host());
    }

    @Test
    void emptyWhenNeitherConfigured() {
        assertTrue(JanusServer.parseStaticDownstreams("", "", 8080, "ws").isEmpty());
        assertTrue(JanusServer.parseStaticDownstreams(null, null, 8080, "ws").isEmpty());
        assertTrue(JanusServer.parseStaticDownstreams("  ", "", 8080, "ws").isEmpty());
    }

    @Test
    void protocolIsPropagatedToEveryInstance() {
        List<ServiceInstance> out = JanusServer.parseStaticDownstreams(
                "a:1,b:2", "", 8080, "binary-proto");
        assertTrue(out.stream().allMatch(i -> "binary-proto".equals(i.protocol())));
    }
}
