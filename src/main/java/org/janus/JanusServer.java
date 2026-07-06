package org.janus;

import io.opentelemetry.api.OpenTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.config.ServerConfig;
import org.janus.discovery.*;
import org.janus.grpc.JanusGrpcClient;
import org.janus.grpc.JanusGrpcServer;
import org.janus.grpc.JanusServiceImpl;
import org.janus.handler.ChainHandler;
import org.janus.observability.OtelSupport;
import org.janus.observability.TracingHelper;
import org.janus.ws.JanusWsServer;
import org.janus.ws.JanusWsClient;

import java.util.concurrent.TimeUnit;

/**
 * Janus Server — a unified server supporting WebSocket (JSON + Binary) and gRPC (4 models).
 *
 * Request chain:
 *   Postman → Server 1 (WS→gRPC) → [Nacos] → Server 2 (gRPC→WS) → [etcd] → Server 3 (WS→local)
 *
 * Each instance starts both WS and gRPC servers. The role (entry/middle/terminal)
 * is configured via environment variables:
 *
 *   JANUS_DOWNSTREAM_PROTOCOL  - ws, grpc, none
 *   JANUS_DOWNSTREAM_DISCOVERY - nacos, etcd, none
 *   JANUS_REGISTER             - nacos, etcd, none
 *   JANUS_REGISTER_PROTOCOL    - ws, grpc (how upstreams reach this node)
 *
 * Observability:
 *   JANUS_OTEL_ENABLED         - Y/N (default Y)
 *   OTEL_EXPORTER_OTLP_ENDPOINT   - OTLP endpoint (e.g., http://jaeger:4317)
 *   JANUS_METRICS_PORT         - Prometheus metrics port (default 9100)
 */
public class JanusServer {
    private static final Logger log = LoggerFactory.getLogger(JanusServer.class);

    private JanusWsServer wsServer;
    private JanusGrpcServer grpcServer;
    private JanusGrpcClient grpcClient;
    private JanusWsClient wsClient;
    private ServiceRegistry registry;
    private ServiceRegistry discoveryRegistry;
    private TracingHelper tracingHelper;
    // Ensures stop() runs at most once (shutdown hook + explicit call).
    private final java.util.concurrent.atomic.AtomicBoolean stopped =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    public void start() throws Exception {
        log.info("╔════════════════════════════════════════════════════════════════");
        log.info("║ Janus Server starting");
        log.info("║   WS Port:         {}", ServerConfig.WS_PORT);
        log.info("║   gRPC Port:       {}", ServerConfig.GRPC_PORT);
        log.info("║   Metrics Port:    {}", ServerConfig.METRICS_PORT);
        log.info("║   Server ID:       {}", ServerConfig.SERVER_ID);
        log.info("║   Advertised Host: {}", ServerConfig.ADVERTISED_HOST);
        log.info("║   Downstream:      {} via {}", ServerConfig.DOWNSTREAM_PROTOCOL, ServerConfig.DOWNSTREAM_DISCOVERY);
        log.info("║   Register:        {}", ServerConfig.REGISTER);
        log.info("║   OTel Enabled:    {}", ServerConfig.OTEL_ENABLED);
        log.info("║   OTel Endpoint:   {}", ServerConfig.OTEL_ENDPOINT);
        log.info("╚════════════════════════════════════════════════════════════════");

        // 1. Initialize OpenTelemetry
        OpenTelemetry otel = OtelSupport.initOtel(ServerConfig.OTEL_SERVICE_NAME + "-" + ServerConfig.SERVER_ID);
        tracingHelper = new TracingHelper(otel);

        // 2. Create service registries
        createRegistries();

        // 3. Create gRPC components
        JanusServiceImpl grpcService = new JanusServiceImpl();
        grpcServer = new JanusGrpcServer(grpcService);

        // 4. Connect downstream gRPC client if needed
        if (ServerConfig.isGrpcDownstream()) {
            grpcClient = new JanusGrpcClient();
            grpcClient.connect(discoveryRegistry);
            if (grpcClient.isConnected()) {
                grpcService.setBlockingStub(grpcClient.getBlockingStub());
                grpcService.setAsyncStub(grpcClient.getAsyncStub());
                log.info("Downstream gRPC client connected");
            }
        }

        // 5. Create chain handler
        ChainHandler chainHandler = new ChainHandler(tracingHelper, grpcClient);

        // 6. Create WS client for downstream forwarding if needed
        if (ServerConfig.isWsDownstream() && discoveryRegistry != null) {
            wsClient = new JanusWsClient(discoveryRegistry, ServerConfig.DOWNSTREAM_SERVICE);
            wsClient.connect();
            chainHandler.setWsClient(wsClient);
        }

        // 6b. Let the gRPC entry path route through the chain handler, so a node
        //     that receives on gRPC can forward downstream over WebSocket
        //     (gRPC-in → WS-out middle node) or process locally at a terminal node.
        grpcService.setChainHandler(chainHandler);

        // 7. Start servers — must be listening BEFORE registering into discovery
        grpcServer.start();
        wsServer = new JanusWsServer(ServerConfig.WS_PORT, chainHandler);
        // reuse-addr is configured inside the server constructor
        wsServer.start();

        // 8. Register this server only after both listeners are ready,
        //    to avoid a startup race where peers connect before we accept.
        registerService();

        log.info("╔════════════════════════════════════════════════════════════════");
        log.info("║ Janus Server started successfully");
        log.info("║   WebSocket:  ws://{}:{}/json  (JSON mode)", ServerConfig.ADVERTISED_HOST, ServerConfig.WS_PORT);
        log.info("║   WebSocket:  ws://{}:{}/binary (Binary mode)", ServerConfig.ADVERTISED_HOST, ServerConfig.WS_PORT);
        log.info("║   gRPC:       {}:{}", ServerConfig.ADVERTISED_HOST, ServerConfig.GRPC_PORT);
        log.info("║   Metrics:    http://{}:{}/metrics", ServerConfig.ADVERTISED_HOST, ServerConfig.METRICS_PORT);
        log.info("╚════════════════════════════════════════════════════════════════");

        // 9. Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "shutdown-hook"));
    }

    private void createRegistries() {
        // Registry for registration
        if (ServerConfig.registerNacos()) {
            registry = new NacosRegistry(ServerConfig.NACOS_ENDPOINT);
        } else if (ServerConfig.registerEtcd()) {
            registry = new EtcdRegistry(ServerConfig.ETCD_ENDPOINT);
        }

        // Registry for discovery
        if (ServerConfig.isNacosDiscovery()) {
            discoveryRegistry = new NacosRegistry(ServerConfig.NACOS_ENDPOINT);
        } else if (ServerConfig.isEtcdDiscovery()) {
            discoveryRegistry = new EtcdRegistry(ServerConfig.ETCD_ENDPOINT);
        }
    }

    private void registerService() {
        if (registry == null) {
            log.info("No registration configured");
            return;
        }

        // The advertised protocol/port reflects how this node's UPSTREAM reaches
        // it, and is independent of which registry (nacos/etcd) is used:
        //   S2 registers in Nacos but is reached via gRPC → advertise GRPC_PORT.
        //   S3 registers in etcd  but is reached via WS   → advertise WS_PORT.
        // Controlled by JANUS_REGISTER_PROTOCOL (see ServerConfig).
        String protocol = ServerConfig.REGISTER_PROTOCOL;
        int port = ServerConfig.registerAsGrpc() ? ServerConfig.GRPC_PORT : ServerConfig.WS_PORT;
        registry.register(ServerConfig.SVC_DISC_NAME, ServerConfig.ADVERTISED_HOST, port, protocol);
    }

    public void stop() {
        // Guard against double execution (e.g. the JVM shutdown hook firing while
        // an explicit stop() is already in progress).
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutting down Janus Server (graceful)...");

        // 1. Deregister FIRST so discovery stops handing this node to upstreams.
        //    etcd issues a DELETE and Nacos a deregister; peers' resolvers observe
        //    it and steer new traffic away before we stop serving.
        if (registry != null) {
            try {
                registry.deregister(ServerConfig.SVC_DISC_NAME);
            } catch (Exception e) {
                log.warn("Error deregistering from discovery", e);
            }
        }

        // 2. Drain window: let upstreams notice the deregistration and let in-flight
        //    requests complete before we close the listeners. During this window the
        //    node still serves and can still use its downstream clients.
        long drain = ServerConfig.SHUTDOWN_DRAIN_MS;
        if (drain > 0) {
            log.info("Draining for {} ms before closing listeners", drain);
            try {
                Thread.sleep(drain);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 3. Stop inbound listeners: stop accepting new work and drain the handler
        //    pool. gRPC's stop() flips health to NOT_SERVING and awaits in-flight
        //    calls before forcing termination.
        if (wsServer != null) {
            try { wsServer.stop(1000); } catch (Exception e) { log.warn("Error stopping WS server", e); }
        }
        if (grpcServer != null) {
            grpcServer.stop();
        }

        // 4. Close downstream clients only after inbound has drained, so forwards
        //    issued during the drain window still had a working channel/pool.
        if (wsClient != null) {
            wsClient.shutdown();
        }
        if (grpcClient != null) {
            grpcClient.shutdown();
        }

        // 5. Close registry connections and flush observability last.
        if (registry != null) {
            try { registry.close(); } catch (Exception ignored) {}
        }
        if (discoveryRegistry != null && discoveryRegistry != registry) {
            try { discoveryRegistry.close(); } catch (Exception ignored) {}
        }
        OtelSupport.shutdown();

        log.info("Janus Server stopped");
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.blockUntilShutdown();
        } else {
            Thread.currentThread().join();
        }
    }

    public static void main(String[] args) {
        JanusServer server = new JanusServer();
        try {
            server.start();
            server.blockUntilShutdown();
        } catch (Exception e) {
            log.error("Failed to start server", e);
            System.exit(1);
        }
    }
}
