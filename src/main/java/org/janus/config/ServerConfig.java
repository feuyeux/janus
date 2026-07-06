package org.janus.config;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class ServerConfig {

    // Server ports
    public static final int WS_PORT = getIntEnv("JANUS_WS_PORT", 8080);
    public static final int GRPC_PORT = getIntEnv("JANUS_GRPC_PORT", 9090);
    public static final int METRICS_PORT = getIntEnv("JANUS_METRICS_PORT", 9100);

    // Server identity
    // System.getProperty("pid") is not a standard JVM property (always absent),
    // so the previous default silently degraded to "janus-0". Use the real PID.
    public static final String SERVER_ID = getEnv("JANUS_SERVER_ID", "janus-" + ProcessHandle.current().pid());
    public static final String HOST = getEnv("JANUS_HOST", "0.0.0.0");
    public static final String ADVERTISED_HOST = getEnv("JANUS_ADVERTISED_HOST", "localhost");

    // Service discovery name
    public static final String SVC_DISC_NAME = "janus-server";

    // Optional shared-secret auth for the WebSocket entry point.
    // Empty (the default) disables the check entirely, preserving the existing
    // open-by-default behaviour. When set, inbound WS handshakes must present a
    // matching "authToken" header, and the downstream WS forwarder sends it too.
    public static final String AUTH_TOKEN = getEnv("JANUS_AUTH_TOKEN", "");

    public static boolean authEnabled() {
        return !AUTH_TOKEN.isEmpty();
    }

    /**
     * Constant-time comparison of a presented token against the configured one.
     * Avoids leaking the token length/prefix through response timing (a plain
     * {@code String.equals} short-circuits on the first mismatching char).
     */
    public static boolean authTokenMatches(String presented) {
        if (presented == null) return false;
        byte[] a = AUTH_TOKEN.getBytes(StandardCharsets.UTF_8);
        byte[] b = presented.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(a, b);
    }

    // Downstream configuration
    public static final String DOWNSTREAM_PROTOCOL = getEnv("JANUS_DOWNSTREAM_PROTOCOL", "none"); // ws, grpc, none
    public static final String DOWNSTREAM_DISCOVERY = getEnv("JANUS_DOWNSTREAM_DISCOVERY", "none"); // nacos, etcd, none
    public static final String DOWNSTREAM_SERVICE = getEnv("JANUS_DOWNSTREAM_SERVICE", SVC_DISC_NAME);
    // Wire encoding used by the WS forwarding client: "json" (text frames on
    // /json) or "binary" (MSG_JANUS frames on /binary). Only meaningful when
    // DOWNSTREAM_PROTOCOL=ws. Defaults to json.
    public static final String DOWNSTREAM_WS_MODE = getEnv("JANUS_DOWNSTREAM_WS_MODE", "json"); // json, binary
    // Which WS protocols this node's server accepts: "json" (only /json),
    // "binary" (only /binary) or "both" (default). Lets a node advertise a single
    // wire format — e.g. S1 json-only, S3 binary-only.
    public static final String WS_MODE = getEnv("JANUS_WS_MODE", "both"); // json, binary, both
    // Registration
    public static final String REGISTER = getEnv("JANUS_REGISTER", "none"); // nacos, etcd, none
    // Protocol this node advertises when registering — i.e. how its UPSTREAM
    // reaches it. Decoupled from the registry type (nacos/etcd): with the
    // gRPC-in→WS-out middle node, S2 registers in Nacos but is reached via gRPC,
    // and S3 registers in etcd but is reached via WS. The advertised port follows
    // this protocol (grpc → GRPC_PORT, ws → WS_PORT). Defaults preserve the
    // historical coupling (etcd→grpc, nacos→ws) when left unset.
    public static final String REGISTER_PROTOCOL = getEnv("JANUS_REGISTER_PROTOCOL",
            "etcd".equalsIgnoreCase(REGISTER) ? "grpc" : "ws"); // ws, grpc

    // Discovery endpoints
    public static final String NACOS_ENDPOINT = getEnv("JANUS_NACOS_ENDPOINT", "localhost:8848");
    public static final String ETCD_ENDPOINT = getEnv("JANUS_ETCD_ENDPOINT", "http://localhost:2379");

    // Observability
    public static final boolean OTEL_ENABLED = "Y".equalsIgnoreCase(getEnv("JANUS_OTEL_ENABLED", "Y"));
    public static final String OTEL_ENDPOINT = getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
    // Base service name; the code appends "-{SERVER_ID}". Kept as "janus" to
    // match docker-compose and the documented default.
    public static final String OTEL_SERVICE_NAME = getEnv("OTEL_SERVICE_NAME", "janus");

    // etcd lease TTL in seconds
    public static final long ETCD_TTL = 10L;

    // ── Concurrency / throughput tuning ───────────────────────────────────────
    // Number of multiplexed WS connections in the downstream forwarding pool.
    // Each connection carries many concurrent in-flight requests (correlated by
    // request_id), so this mainly spreads load across TCP connections/instances.
    public static final int WS_POOL_SIZE = Math.max(1, getIntEnv("JANUS_WS_POOL_SIZE", 8));
    // Per-request timeout (ms) for a downstream WS round-trip.
    public static final long WS_FORWARD_TIMEOUT_MS = getLongEnv("JANUS_WS_FORWARD_TIMEOUT_MS", 10_000L);
    // Fast dead-connection detection for both the WS server and the multiplexed
    // WS forwarding links. Java-WebSocket periodically sends protocol-level ping
    // frames and closes a connection if no pong arrives within this window. A
    // value smaller than the library default (60s) surfaces half-open/partitioned
    // peers quickly, so the pool can evict and reconnect and callers fail fast
    // instead of writing into a black hole. 0 disables the library's lost-
    // connection detection entirely.
    public static final int WS_CONN_LOST_TIMEOUT_SEC = getIntEnv("JANUS_WS_CONN_LOST_TIMEOUT_SEC", 20);
    // Upper bound for the fallback platform-thread handler pools (ignored when
    // virtual threads are available at runtime).
    public static final int HANDLER_MAX_THREADS = getIntEnv("JANUS_HANDLER_MAX_THREADS", 512);
    // gRPC max inbound message size (bytes) and concurrent streams per connection.
    public static final int GRPC_MAX_INBOUND_MSG = getIntEnv("JANUS_GRPC_MAX_INBOUND_MSG", 16 * 1024 * 1024);
    public static final int GRPC_MAX_CONCURRENT_CALLS = getIntEnv("JANUS_GRPC_MAX_CONCURRENT_CALLS", 0); // 0 = unlimited
    // gRPC HTTP/2 flow-control window (bytes) — larger windows raise throughput
    // for streaming/high-BDP links.
    public static final int GRPC_FLOW_WINDOW = getIntEnv("JANUS_GRPC_FLOW_WINDOW", 8 * 1024 * 1024);
    // Expose gRPC server reflection (handy for grpcurl in the demo). Disable in
    // hardened deployments to reduce information exposure.
    public static final boolean GRPC_REFLECTION_ENABLED =
            "Y".equalsIgnoreCase(getEnv("JANUS_GRPC_REFLECTION", "Y"));

    // ── Graceful shutdown ──────────────────────────────────────────────────────
    // Drain window (ms) applied on shutdown AFTER deregistering from discovery and
    // BEFORE closing the inbound listeners: it gives upstreams time to observe the
    // deregistration (etcd DELETE / Nacos deregister) and lets in-flight requests
    // finish, so stopping a node does not surface errors to callers. 0 disables the
    // pause (useful for fast local restarts / tests).
    public static final long SHUTDOWN_DRAIN_MS = getLongEnv("JANUS_SHUTDOWN_DRAIN_MS", 2000L);

    // ── Optional TLS (opt-in; disabled by default, so the plaintext demo stack
    // and tests are unaffected) ────────────────────────────────────────────────
    // When enabled, gRPC uses TLS (PEM cert/key/CA) and WS uses wss (keystore).
    public static final boolean TLS_ENABLED = "Y".equalsIgnoreCase(getEnv("JANUS_TLS_ENABLED", "N"));
    // Require peer (client) certificates — mutual TLS.
    public static final boolean TLS_MTLS = "Y".equalsIgnoreCase(getEnv("JANUS_TLS_MTLS", "N"));
    // gRPC TLS material (PEM). CERT is the server/identity chain, KEY its PKCS8
    // private key, CA the trust roots for verifying peers.
    public static final String TLS_CERT = getEnv("JANUS_TLS_CERT", "");
    public static final String TLS_KEY = getEnv("JANUS_TLS_KEY", "");
    public static final String TLS_CA = getEnv("JANUS_TLS_CA", "");
    // WS TLS material (Java keystore/truststore) for the javax SSLContext.
    public static final String TLS_KEYSTORE = getEnv("JANUS_TLS_KEYSTORE", "");
    public static final String TLS_KEYSTORE_PASSWORD = getEnv("JANUS_TLS_KEYSTORE_PASSWORD", "");
    public static final String TLS_KEYSTORE_TYPE = getEnv("JANUS_TLS_KEYSTORE_TYPE", "PKCS12");
    public static final String TLS_TRUSTSTORE = getEnv("JANUS_TLS_TRUSTSTORE", "");
    public static final String TLS_TRUSTSTORE_PASSWORD = getEnv("JANUS_TLS_TRUSTSTORE_PASSWORD", "");

    public static boolean tlsEnabled() {
        return TLS_ENABLED;
    }

    private ServerConfig() {}

    private static String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val == null || val.isEmpty()) ? defaultValue : val;
    }

    private static int getIntEnv(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long getLongEnv(String key, long defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty()) return defaultValue;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static boolean hasDownstream() {
        return !"none".equalsIgnoreCase(DOWNSTREAM_PROTOCOL);
    }

    public static boolean isWsDownstream() {
        return "ws".equalsIgnoreCase(DOWNSTREAM_PROTOCOL);
    }

    public static boolean isGrpcDownstream() {
        return "grpc".equalsIgnoreCase(DOWNSTREAM_PROTOCOL);
    }

    /** WS server accepts JSON (/json). True unless restricted to binary-only. */
    public static boolean wsJsonEnabled() {
        return !"binary".equalsIgnoreCase(WS_MODE);
    }

    /** WS server accepts Binary (/binary). True unless restricted to json-only. */
    public static boolean wsBinaryEnabled() {
        return !"json".equalsIgnoreCase(WS_MODE);
    }

    public static boolean isNacosDiscovery() {
        return "nacos".equalsIgnoreCase(DOWNSTREAM_DISCOVERY);
    }

    public static boolean isEtcdDiscovery() {
        return "etcd".equalsIgnoreCase(DOWNSTREAM_DISCOVERY);
    }

    public static boolean registerNacos() {
        return "nacos".equalsIgnoreCase(REGISTER);
    }

    public static boolean registerEtcd() {
        return "etcd".equalsIgnoreCase(REGISTER);
    }

    /** True when this node advertises its gRPC endpoint to upstreams. */
    public static boolean registerAsGrpc() {
        return "grpc".equalsIgnoreCase(REGISTER_PROTOCOL);
    }
}
