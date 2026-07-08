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
    // When true, forward as MSG_JANUS binary frames on the /binary endpoint
    // (the downstream only speaks binary); otherwise JSON text on /json.
    private final boolean binaryMode;

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
        this(registry, downstreamService, ServerConfig.DOWNSTREAM_WS_MODE);
    }

    public JanusWsClient(ServiceRegistry registry, String downstreamService, String wsMode) {
        this.registry = registry;
        this.downstreamService = downstreamService;
        this.poolSize = ServerConfig.WS_POOL_SIZE;
        this.timeoutMs = ServerConfig.WS_FORWARD_TIMEOUT_MS;
        this.binaryMode = "binary".equalsIgnoreCase(wsMode);
    }

    /** True when this client forwards using the WS Binary protocol. */
    public boolean isBinary() {
        return binaryMode;
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
        String path = binaryMode ? "/binary" : "/json";
        int needed = poolSize - pool.size();
        for (int i = 0; i < needed; i++) {
            ServiceRegistry.ServiceInstance inst = instances.get(i % instances.size());
            String url = scheme + "://" + inst.host() + ":" + inst.port() + path;
            MultiplexClient c = MultiplexClient.connectWithRetry(URI.create(url), 2);
            if (c != null) {
                pool.add(c);
                log.info("WS forwarding connection established: {}", url);
            } else {
                log.warn("WS forwarding connection failed: {}", url);
            }
        }
        started = !pool.isEmpty();
        log.debug("WS forwarding pool size: {} (target {})", pool.size(), poolSize);
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
        return (String) roundTripLoop(correlationId, jsonRequest);
    }

    /**
     * Forward a MSG_JANUS binary frame and await the reply matching
     * {@code correlationId}. The reply's raw bytes are returned for the caller to
     * decode.
     *
     * @throws RuntimeException on timeout, downstream error, or no available connection
     */
    public byte[] forwardBinary(String correlationId, byte[] frame) {
        return (byte[]) roundTripLoop(correlationId, frame);
    }

    /**
     * Shared send/await/retry loop for both JSON (String payload) and Binary
     * (byte[] payload) forwarding. The reply object type mirrors the request.
     */
    private Object roundTripLoop(String correlationId, Object payload) {
        NotSentException lastNotSent = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            MultiplexClient client = pickHealthy();
            if (client == null) {
                triggerRefill(); // reconnect happens in the background
                throw new RuntimeException("No downstream WS connection available");
            }
            try {
                return client.roundTrip(correlationId, payload, timeoutMs);
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
            } finally {
                // Release the slot reserved by pickHealthy() on every outcome
                // (success, timeout, interrupt, not-sent, or a post-send error that
                // propagates out uncaught), keeping the in-flight counter balanced.
                client.release();
            }
            // NOTE: a post-send failure surfaces as a plain RuntimeException from
            // roundTrip and is intentionally NOT caught here, so it propagates
            // without a retry (avoids duplicate downstream processing).
        }
        throw new RuntimeException("WS forward failed: no healthy connection",
                lastNotSent != null ? lastNotSent.getCause() : null);
    }

    /**
     * Pick a connection for the next request by <b>least in-flight requests</b>
     * and immediately <em>reserve</em> a slot on it.
     *
     * <p>These connections are <em>multiplexed</em>: each carries many concurrent
     * requests correlated by {@code request_id}. Balancing therefore must count
     * <b>requests</b>, not connections — the connection-count metric (nginx
     * {@code least_conn}) is decoupled from real request load here, because one
     * connection can hold hundreds of in-flight requests while another sits idle.
     * Selecting the least-loaded connection is the multiplexing-correct analog of
     * "least connections" and spreads request load evenly across the pool (and,
     * when the pool fans out across discovered backends, across those backends).
     *
     * <p><b>Race-free assignment:</b> the chosen connection's in-flight counter is
     * incremented ({@link MultiplexClient#reserve()}) before this method returns,
     * so a burst of concurrent callers no longer all observe the same connection
     * as idle and pile onto it — each caller's reservation is visible to the next.
     * The reservation is released in {@link #roundTripLoop} once the round-trip
     * completes (on any outcome). A rotating start index breaks ties fairly so
     * equal-load connections are not all served from slot 0.
     */
    private MultiplexClient pickHealthy() {
        MultiplexClient[] snapshot = pool.toArray(new MultiplexClient[0]);
        int n = snapshot.length;
        if (n == 0) {
            return null;
        }
        int[] loads = new int[n];
        boolean[] open = new boolean[n];
        for (int i = 0; i < n; i++) {
            open[i] = snapshot[i].isOpen();
            loads[i] = snapshot[i].inFlight();
        }
        int start = Math.floorMod(roundRobin.getAndIncrement(), n);
        int idx = selectLeastLoadedIndex(loads, open, start);
        if (idx < 0) {
            return null;
        }
        MultiplexClient chosen = snapshot[idx];
        chosen.reserve(); // reserve immediately so concurrent selectors see the load
        return chosen;
    }

    /**
     * Pure selection core (package-private for testing): return the index of the
     * open connection with the fewest in-flight requests, scanning from
     * {@code start} so ties rotate. Returns {@code -1} when no connection is open.
     * An idle (load 0) connection short-circuits the scan since nothing can beat it.
     */
    static int selectLeastLoadedIndex(int[] loads, boolean[] open, int start) {
        int n = loads.length;
        int best = -1;
        int bestLoad = Integer.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            int idx = (start + i) % n;
            if (!open[idx]) {
                continue;
            }
            if (loads[idx] < bestLoad) {
                best = idx;
                bestLoad = loads[idx];
                if (bestLoad == 0) {
                    break; // cannot do better than an idle connection
                }
            }
        }
        return best;
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

        private final ConcurrentHashMap<String, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();

        // Requests assigned to this connection but not yet completed. Incremented
        // at selection time (reserve) rather than at send time so concurrent
        // selectors observe the load immediately — closing the pick race where
        // many callers would otherwise all target the same "idle" connection.
        private final AtomicInteger inFlight = new AtomicInteger();

        /** Number of requests currently reserved/in-flight on this connection. */
        int inFlight() {
            return inFlight.get();
        }

        /** Reserve a slot the instant this connection is chosen (see pickHealthy). */
        void reserve() {
            inFlight.incrementAndGet();
        }

        /** Release the slot when the round-trip completes (any outcome). */
        void release() {
            inFlight.decrementAndGet();
        }

        MultiplexClient(URI serverUri) {
            super(serverUri);
            addHeader("userId", "janus-ws-client-" + UUID.randomUUID().toString().substring(0, 8));
            // Fast liveness detection so a dead/half-open downstream link is closed
            // (triggering onClose → failAllPending + pool eviction) well before the
            // 60s library default, letting the maintainer reconnect promptly.
            if (ServerConfig.WS_CONN_LOST_TIMEOUT_SEC > 0) {
                setConnectionLostTimeout(ServerConfig.WS_CONN_LOST_TIMEOUT_SEC);
            }
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

        static MultiplexClient connectWithRetry(URI serverUri, int attempts) {
            for (int a = 1; a <= attempts; a++) {
                MultiplexClient client = new MultiplexClient(serverUri);
                try {
                    if (client.connectBlocking(5, TimeUnit.SECONDS) && client.isOpen()) {
                        return client;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                } catch (RuntimeException e) {
                    log.debug("WS forwarding connect attempt failed for {}: {}", serverUri, e.getMessage());
                }
                try {
                    client.close();
                } catch (Exception ignored) {
                    // failed WebSocketClient instances cannot be reused
                }
                if (a < attempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
            return null;
        }

        Object roundTrip(String correlationId, Object payload, long timeoutMs)
                throws InterruptedException, TimeoutException {
            CompletableFuture<Object> future = new CompletableFuture<>();
            pending.put(correlationId, future);
            try {
                // WebsocketNotConnectedException (unchecked) if the socket dropped.
                if (payload instanceof byte[] bytes) {
                    send(bytes);
                } else {
                    send((String) payload);
                }
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
            CompletableFuture<Object> future = pending.remove(corrId);
            if (future != null) {
                future.complete(message);
            } else {
                log.warn("No pending WS request for corr={} (late/duplicate reply)", corrId);
            }
        }

        @Override
        public void onMessage(ByteBuffer message) {
            // Binary forwarding reply: decode the MSG_JANUS frame just enough to
            // read its correlation id, then hand the raw bytes to the waiter for
            // full decoding.
            byte[] bytes = new byte[message.remaining()];
            message.get(bytes);
            // The downstream greets new /binary connections with a BONJOUR frame
            // (and may emit other non-JANUS control frames); ignore anything that
            // is not a MSG_JANUS reply rather than logging it as an error.
            if (bytes.length < BinaryCodec.HEADER_LEN || bytes[2] != BinaryCodec.MSG_JANUS) {
                log.debug("WS forward: ignoring non-JANUS binary frame (type=0x{})",
                        bytes.length >= BinaryCodec.HEADER_LEN ? String.format("%02x", bytes[2]) : "??");
                return;
            }
            String corrId;
            try {
                corrId = BinaryCodec.decodeJanus(bytes).requestId();
            } catch (BinaryCodec.DecodeException e) {
                log.warn("WS forward binary reply undecodable; dropping: {}", e.getMessage());
                return;
            }
            if (corrId == null || corrId.isEmpty()) {
                log.warn("WS forward binary reply without request_id; dropping");
                return;
            }
            CompletableFuture<Object> future = pending.remove(corrId);
            if (future != null) {
                future.complete(bytes);
            } else {
                log.warn("No pending WS request for corr={} (late/duplicate binary reply)", corrId);
            }
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
            for (Map.Entry<String, CompletableFuture<Object>> e : pending.entrySet()) {
                CompletableFuture<Object> f = pending.remove(e.getKey());
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
