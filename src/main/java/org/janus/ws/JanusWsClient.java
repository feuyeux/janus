package org.janus.ws;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.codec.BinaryCodec;
import org.janus.config.ServerConfig;
import org.janus.discovery.ServiceRegistry;
import org.janus.security.TlsSupport;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebSocket client for forwarding requests to a downstream WS server.
 *
 * <p><b>High concurrency / throughput / low latency:</b> a <em>pool of
 * multiplexed connections</em> where each connection carries many concurrent
 * in-flight requests correlated by the envelope's {@code request_id}. A
 * per-request {@link CompletableFuture} is registered before sending and
 * completed when the reply with the matching id arrives.
 *
 * <p><b>Non-blocking hot path (R4):</b> {@link #forward} only ever picks an
 * already-open connection; (re)connection and discovery happen on a single
 * background maintainer thread, so a downstream outage never makes many handler
 * threads serialise on blocking reconnects — requests fail fast instead.
 *
 * <p><b>Safe retry (R1):</b> a request is only retried on another connection
 * when it provably was <em>not</em> transmitted (the {@code send} itself
 * failed). Once bytes are on the wire, a mid-flight failure is surfaced to the
 * caller rather than silently re-sent, avoiding duplicate downstream processing
 * of non-idempotent requests.
 */
public class JanusWsClient {
    private static final Logger log = LoggerFactory.getLogger(JanusWsClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String downstreamService;
    private final ServiceRegistry registry;
    private final int poolSize;
    private final long timeoutMs;

    private final List<MultiplexClient> pool = new CopyOnWriteArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private volatile boolean started = false;

    private final ScheduledExecutorService maintainer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-pool-maintainer");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean maintainerStarted = false;

    public JanusWsClient(ServiceRegistry registry, String downstreamService) {
        this.registry = registry;
        this.downstreamService = downstreamService;
        this.poolSize = ServerConfig.WS_POOL_SIZE;
        this.timeoutMs = ServerConfig.WS_FORWARD_TIMEOUT_MS;
    }

    /** Establish the pool synchronously once, then keep it healthy in the background. */
    public void connect() {
        ensurePool();
        startMaintainer();
    }

    private synchronized void startMaintainer() {
        if (maintainerStarted) {
            return;
        }
        maintainerStarted = true;
        // Periodic refill/reconnect off the request hot path.
        maintainer.scheduleWithFixedDelay(() -> {
            try {
                ensurePool();
            } catch (Exception e) {
                log.warn("WS pool maintenance error", e);
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    /** Kick an immediate, asynchronous pool refill (non-blocking). */
    private void triggerRefill() {
        try {
            maintainer.execute(this::ensurePool);
        } catch (RejectedExecutionException ignored) {
            // maintainer shutting down
        }
    }

    /**
     * Discover instances and fill any missing connection slots. Runs on the
     * caller's thread only at startup; afterwards exclusively on the maintainer
     * thread, so blocking connect attempts never touch request handler threads.
     */
    private synchronized void ensurePool() {
        if (registry == null) {
            log.error("No registry configured for WS client");
            return;
        }
        pool.removeIf(c -> !c.isOpen());

        List<ServiceRegistry.ServiceInstance> instances = registry.discover(downstreamService);
        if (instances.isEmpty()) {
            if (pool.isEmpty()) {
                log.warn("No downstream WS instances discovered for [{}]", downstreamService);
            }
            started = !pool.isEmpty();
            return;
        }

        String scheme = ServerConfig.tlsEnabled() ? "wss" : "ws";
        int needed = poolSize - pool.size();
        for (int i = 0; i < needed; i++) {
            ServiceRegistry.ServiceInstance inst = instances.get(i % instances.size());
            String url = scheme + "://" + inst.host() + ":" + inst.port() + "/json";
            MultiplexClient c = new MultiplexClient(URI.create(url));
            if (c.connectWithRetry(2)) {
                pool.add(c);
                log.info("WS forwarding connection established: {}", url);
            } else {
                log.warn("WS forwarding connection failed: {}", url);
            }
        }
        started = !pool.isEmpty();
        log.info("WS forwarding pool size: {} (target {})", pool.size(), poolSize);
    }

    public boolean isConnected() {
        if (!started) {
            return false;
        }
        for (MultiplexClient c : pool) {
            if (c.isOpen()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Forward a JSON request and await the reply matching {@code correlationId}.
     *
     * @throws RuntimeException on timeout, downstream error, or no available connection
     */
    public String forward(String correlationId, String jsonRequest) {
        NotSentException lastNotSent = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            MultiplexClient client = pickHealthy();
            if (client == null) {
                triggerRefill(); // reconnect happens in the background
                throw new RuntimeException("No downstream WS connection available");
            }
            try {
                return client.roundTrip(correlationId, jsonRequest, timeoutMs);
            } catch (TimeoutException te) {
                // Sent but no reply in time — do not resend (may be processing).
                throw new RuntimeException("Timeout waiting for downstream WS response (corr=" + correlationId + ")");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for downstream WS response");
            } catch (NotSentException nse) {
                // Nothing was transmitted on this connection; safe to try another.
                lastNotSent = nse;
                triggerRefill();
            }
            // NOTE: a post-send failure surfaces as a plain RuntimeException from
            // roundTrip and is intentionally NOT caught here, so it propagates
            // without a retry (avoids duplicate downstream processing).
        }
        throw new RuntimeException("WS forward failed: no healthy connection",
                lastNotSent != null ? lastNotSent.getCause() : null);
    }

    private MultiplexClient pickHealthy() {
        MultiplexClient[] snapshot = pool.toArray(new MultiplexClient[0]);
        int n = snapshot.length;
        for (int i = 0; i < n; i++) {
            MultiplexClient c = snapshot[Math.floorMod(roundRobin.getAndIncrement(), n)];
            if (c.isOpen()) {
                return c;
            }
        }
        return null;
    }

    public void shutdown() {
        maintainer.shutdownNow();
        for (MultiplexClient c : pool) {
            try {
                if (c.isOpen()) {
                    c.send(BinaryCodec.disconnect("shutdown").encode());
                }
            } catch (Exception ignored) {
                // best effort
            }
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        pool.clear();
        started = false;
    }

    /** Marker for "the request never left this process" — the only retryable case. */
    private static final class NotSentException extends RuntimeException {
        NotSentException(Throwable cause) {
            super(cause);
        }
    }

    // ─── Inner multiplexed client ─────────────────────────────────────────────

    static class MultiplexClient extends WebSocketClient {
        private static final Logger log = LoggerFactory.getLogger(MultiplexClient.class);

        private final ConcurrentHashMap<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();

        MultiplexClient(URI serverUri) {
            super(serverUri);
            addHeader("userId", "janus-ws-client-" + UUID.randomUUID().toString().substring(0, 8));
            // Propagate the shared secret so the downstream node accepts us when
            // auth is enabled. No-op when auth is disabled (empty token).
            if (ServerConfig.authEnabled()) {
                addHeader("authToken", ServerConfig.AUTH_TOKEN);
            }
            // Opt-in TLS (wss). Disabled by default → plain ws.
            if (ServerConfig.tlsEnabled()) {
                try {
                    setSocketFactory(TlsSupport.wsClientSocketFactory());
                } catch (Exception e) {
                    log.error("WS client TLS setup failed for {}: {}", serverUri, e.getMessage());
                }
            }
        }

        boolean connectWithRetry(int attempts) {
            for (int a = 1; a <= attempts; a++) {
                try {
                    if (connectBlocking(5, TimeUnit.SECONDS) && isOpen()) {
                        return true;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                if (a < attempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
            return false;
        }

        String roundTrip(String correlationId, String json, long timeoutMs)
                throws InterruptedException, TimeoutException {
            CompletableFuture<String> future = new CompletableFuture<>();
            pending.put(correlationId, future);
            try {
                send(json); // WebsocketNotConnectedException (unchecked) if the socket dropped
            } catch (RuntimeException e) {
                // Nothing was transmitted — retryable on another connection.
                pending.remove(correlationId);
                throw new NotSentException(e);
            }
            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (ExecutionException ee) {
                // The request WAS sent; downstream may have processed it. Surface
                // the failure without retrying (see forward()).
                Throwable cause = ee.getCause();
                throw new RuntimeException("Downstream WS error after send: "
                        + (cause != null ? cause.getMessage() : ee.getMessage()), cause);
            } finally {
                pending.remove(correlationId);
            }
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            log.info("WS forwarding client connected: {}", getURI());
        }

        @Override
        public void onMessage(String message) {
            String corrId = extractRequestId(message);
            if (corrId == null) {
                log.warn("WS forward reply without request_id; dropping");
                return;
            }
            CompletableFuture<String> future = pending.remove(corrId);
            if (future != null) {
                future.complete(message);
            } else {
                log.warn("No pending WS request for corr={} (late/duplicate reply)", corrId);
            }
        }

        @Override
        public void onMessage(ByteBuffer message) {
            // Binary replies are not used on the JSON forwarding path.
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.info("WS forwarding client closed: {} {}", code, reason);
            failAllPending(new RuntimeException("Connection closed by downstream: " + code + " " + reason));
        }

        @Override
        public void onError(Exception ex) {
            log.error("WS forwarding client error", ex);
            failAllPending(ex);
        }

        private void failAllPending(Throwable t) {
            for (Map.Entry<String, CompletableFuture<String>> e : pending.entrySet()) {
                CompletableFuture<String> f = pending.remove(e.getKey());
                if (f != null) {
                    f.completeExceptionally(t);
                }
            }
        }

        private String extractRequestId(String message) {
            try {
                JsonNode node = objectMapper.readTree(message);
                JsonNode rid = node.get("request_id");
                return (rid != null && !rid.isNull()) ? rid.asText() : null;
            } catch (Exception e) {
                return null;
            }
        }
    }
}
