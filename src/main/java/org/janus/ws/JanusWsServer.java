package org.janus.ws;

import org.java_websocket.WebSocket;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.server.DefaultSSLWebSocketServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.codec.BinaryCodec;
import org.janus.common.ExecutorSupport;
import org.janus.config.ServerConfig;
import org.janus.handler.ChainHandler;
import org.janus.observability.OtelSupport;
import org.janus.security.TlsSupport;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * WebSocket server supporting both JSON and Binary protocols.
 * Path /json → JSON text messages
 * Path /binary → Binary protocol messages
 * Path / → JSON (default)
 *
 * <p>Message handling (which may block on a downstream gRPC call or a WS
 * round-trip) is dispatched to a dedicated executor so the WebSocket I/O
 * threads are never blocked. On JDK 21+ the executor is virtual-thread-based,
 * giving high concurrency and low latency under many simultaneous clients.
 */
public class JanusWsServer extends WebSocketServer {
    private static final Logger log = LoggerFactory.getLogger(JanusWsServer.class);

    private final ChainHandler chainHandler;
    private final ExecutorService handlerExecutor;

    public JanusWsServer(int port, ChainHandler chainHandler) {
        super(new InetSocketAddress(port));
        this.chainHandler = chainHandler;
        this.handlerExecutor = ExecutorSupport.newHandlerExecutor("ws-handler", ServerConfig.HANDLER_MAX_THREADS);
        // Decode/read threads should not block on request processing.
        setReuseAddr(true);
        // Opt-in TLS (wss). Fail fast on misconfiguration rather than serving ws.
        if (ServerConfig.tlsEnabled()) {
            try {
                setWebSocketFactory(new DefaultSSLWebSocketServerFactory(TlsSupport.wsServerSslContext()));
                log.info("WebSocket server TLS (wss) enabled");
            } catch (Exception e) {
                throw new IllegalStateException("WebSocket server TLS setup failed", e);
            }
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String path = handshake.getResourceDescriptor();

        // Optional shared-secret gate. Disabled by default (empty token); when
        // configured, reject handshakes that do not present the matching token.
        // Uses a constant-time comparison to avoid a token-timing side channel.
        if (ServerConfig.authEnabled()) {
            String presented = handshake.getFieldValue("authToken");
            if (!ServerConfig.authTokenMatches(presented)) {
                log.warn("Rejected WS handshake: missing/invalid authToken (path={})", path);
                conn.close(CloseFrame.POLICY_VALIDATION, "unauthorized");
                return;
            }
        }

        String userId = handshake.getFieldValue("userId");
        if (userId == null || userId.isEmpty()) {
            userId = "client-" + UUID.randomUUID().toString().substring(0, 8);
        }
        conn.setAttachment(new Session(userId, path));
        log.info("[{}] session+ path={}", userId, path);
        OtelSupport.recordWsMessage("connect");

        // For binary protocol, send BONJOUR
        if (isBinaryPath(path)) {
            conn.send(BinaryCodec.bonjour("JANUS-JAVA").encode());
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        Session session = conn.getAttachment();
        if (session == null) return;

        log.info("[{}] JSON message: {}", session.userId, message);
        OtelSupport.recordWsMessage("json-text");

        // Offload processing (may block on downstream) off the WS read thread.
        Map<String, String> traceContext = session.traceContext;
        try {
            handlerExecutor.execute(() -> {
                try {
                    String response = chainHandler.handleJsonRequest(message, traceContext);
                    conn.send(response);
                    log.info("[{}] JSON response sent", session.userId);
                } catch (Exception e) {
                    log.error("[{}] Error processing JSON message", session.userId, e);
                }
            });
        } catch (RejectedExecutionException rejected) {
            // Fallback pool saturated (JDK < 21 only). Shed load without blocking
            // the I/O thread.
            log.warn("[{}] handler pool saturated; rejecting JSON message", session.userId);
            conn.send("{\"mode\":\"ERROR\",\"status\":503,\"error_msg\":\"server busy\"}");
        }
    }

    @Override
    public void onMessage(WebSocket conn, java.nio.ByteBuffer message) {
        Session session = conn.getAttachment();
        if (session == null) return;

        byte[] data = new byte[message.remaining()];
        message.get(data);
        try {
            handlerExecutor.execute(() -> {
                try {
                    onBinaryMessage(conn, data, session);
                } catch (Exception e) {
                    log.error("[{}] Error processing binary message", session.userId, e);
                }
            });
        } catch (RejectedExecutionException rejected) {
            log.warn("[{}] handler pool saturated; rejecting binary message", session.userId);
            conn.send(BinaryCodec.error(BinaryCodec.ERR_DECODE, "server busy").encode());
        }
    }

    private void onBinaryMessage(WebSocket conn, byte[] data, Session session) {
        // MSG_JANUS (0x10) carries a different payload layout than the classic
        // messages handled by decodeMessage(), so dispatch it directly from the
        // frame header before attempting a generic decode.
        if (data.length >= BinaryCodec.HEADER_LEN && data[2] == BinaryCodec.MSG_JANUS) {
            OtelSupport.recordWsMessage("binary-" + BinaryCodec.MSG_JANUS);
            try {
                BinaryCodec.JanusFrame frame = BinaryCodec.decodeJanus(data);
                log.info("[{}] MSG_JANUS method={} mode={} seq={} data={}",
                        session.userId, frame.method(), frame.mode(), frame.seq(), frame.data());
                byte[] response = chainHandler.handleBinaryJanus(frame);
                conn.send(response);
            } catch (BinaryCodec.DecodeException e) {
                log.error("[{}] Janus decode error: {}", session.userId, e.getMessage());
                conn.send(BinaryCodec.error(BinaryCodec.ERR_DECODE, e.getMessage()).encode());
            }
            return;
        }

        BinaryCodec.Message msg;
        try {
            msg = BinaryCodec.decodeMessage(data);
        } catch (BinaryCodec.DecodeException e) {
            log.error("[{}] Decode error: {}", session.userId, e.getMessage());
            conn.send(BinaryCodec.error(BinaryCodec.ERR_DECODE, e.getMessage()).encode());
            return;
        }

        OtelSupport.recordWsMessage("binary-" + msg.type);

        switch (msg.type) {
            case BinaryCodec.MSG_HELLO -> {
                session.clientLanguage = msg.clientLanguage;
                log.info("[{}] HELLO from {}", session.userId, msg.clientLanguage);
                conn.send(BinaryCodec.bonjour("JANUS-JAVA").encode());
            }
            case BinaryCodec.MSG_ECHO_REQUEST -> {
                log.info("[{}] ECHO_REQUEST id={} meta={} data={}", session.userId, msg.echoId, msg.echoMeta, msg.echoData);
                byte[] response = chainHandler.handleBinaryEcho(
                        msg.echoId, msg.echoMeta, msg.echoData, msg.traceId, msg.spanId);
                conn.send(response);
            }
            case BinaryCodec.MSG_PING -> {
                conn.send(BinaryCodec.pong(msg.timestampMs).encode());
            }
            case BinaryCodec.MSG_DISCONNECT -> {
                log.info("[{}] DISCONNECT: {}", session.userId, msg.disconnectReason);
                conn.close();
            }
            default -> {
                log.warn("[{}] Unknown message type: 0x{}", session.userId, String.format("%02x", msg.type));
                conn.send(BinaryCodec.error(BinaryCodec.ERR_UNKNOWN_MSG_TYPE,
                        "unknown type 0x" + String.format("%02x", msg.type)).encode());
            }
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        Session session = conn.getAttachment();
        log.info("[{}] session- code={} reason={}",
                session != null ? session.userId : "?", code, reason);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        log.error("WebSocket error: {}", ex.getMessage(), ex);
    }

    @Override
    public void onStart() {
        log.info("WebSocket server started on port {}", getPort());
    }

    @Override
    public void stop(int timeout) throws InterruptedException {
        try {
            super.stop(timeout);
        } finally {
            handlerExecutor.shutdown();
        }
    }

    private boolean isBinaryPath(String path) {
        return path != null && path.startsWith("/binary");
    }

    // ─── Session ─────────────────────────────────────────────────────────────

    static class Session {
        final String userId;
        final String path;
        final String sessionId = UUID.randomUUID().toString();
        volatile String clientLanguage = "unknown";
        final Map<String, String> traceContext = new HashMap<>();

        Session(String userId, String path) {
            this.userId = userId;
            this.path = path;
        }
    }
}
