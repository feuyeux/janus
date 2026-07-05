package org.janus.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.janus.discovery.ServiceRegistry;
import org.janus.handler.ChainHandler;
import org.janus.observability.TracingHelper;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression + concurrency test for the multiplexed WS forwarding client.
 *
 * <p>Before the rewrite, the forwarding client serialised every hop through a
 * single connection + global lock (one in-flight request at a time). This test
 * fires many requests concurrently through the pool and asserts each caller
 * receives exactly its own correlated reply (matched by request_id) with the
 * expected payload — i.e. no cross-talk and no serialization stalls.
 */
class WsForwardingConcurrencyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Minimal registry that always resolves the single local test server. */
    static class StubRegistry implements ServiceRegistry {
        private final String host;
        private final int port;

        StubRegistry(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override public void register(String s, String h, int p, String proto) {}
        @Override public List<ServiceInstance> discover(String s) {
            return List.of(new ServiceInstance(host, port, "ws"));
        }
        @Override public void deregister(String s) {}
        @Override public void close() {}
    }

    @Test
    void concurrentForwardsAreCorrelatedCorrectly() throws Exception {
        int port = freePort();
        // Downstream server: no further downstream → processes locally.
        ChainHandler serverHandler = new ChainHandler(new TracingHelper(OpenTelemetry.noop()), null);
        JanusWsServer server = new JanusWsServer(port, serverHandler);
        server.start();
        waitUntilListening("127.0.0.1", port, 5_000);
        Thread.sleep(150); // let the accept loop settle after bind

        JanusWsClient client = new JanusWsClient(new StubRegistry("127.0.0.1", port), "janus-server");
        client.connect();
        assertTrue(client.isConnected(), "forwarding pool should establish at least one connection");

        final int total = 200;
        ExecutorService pool = Executors.newFixedThreadPool(24);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < total; i++) {
                final int idx = i % 6; // valid greeting indices 0..5
                futures.add(pool.submit(() -> {
                    String corr = UUID.randomUUID().toString();
                    String json = "{\"method\":\"TALK\",\"mode\":\"REQUEST\",\"data\":\"" + idx
                            + "\",\"meta\":\"c\",\"request_id\":\"" + corr + "\"}";
                    String resp = client.forward(corr, json);
                    JsonNode node = MAPPER.readTree(resp);
                    boolean corrOk = corr.equals(node.path("request_id").asText());
                    boolean idxOk = String.valueOf(idx)
                            .equals(node.path("results").get(0).path("kv").path("idx").asText());
                    return corrOk && idxOk;
                }));
            }

            int ok = 0;
            for (Future<Boolean> f : futures) {
                if (Boolean.TRUE.equals(f.get(30, TimeUnit.SECONDS))) {
                    ok++;
                }
            }
            assertEquals(total, ok, "every concurrent forward must return its own correctly-correlated reply");
        } finally {
            pool.shutdownNow();
            client.shutdown();
            server.stop(1000);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void waitUntilListening(String host, int port, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket(host, port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("server did not start listening on " + host + ":" + port);
    }
}
