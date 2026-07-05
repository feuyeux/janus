package org.janus.grpc;

import io.grpc.*;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.common.ExecutorSupport;
import org.janus.config.ServerConfig;
import org.janus.observability.OtelSupport;
import org.janus.security.TlsSupport;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class JanusGrpcServer {
    private static final Logger log = LoggerFactory.getLogger(JanusGrpcServer.class);

    private final Server server;
    private final HealthStatusManager healthStatusManager = new HealthStatusManager();
    private final JanusServiceImpl service;
    // Dedicated handler executor (virtual threads on JDK 21+) so RPC handling and
    // blocking downstream forwards scale to high concurrency with low latency.
    private final ExecutorService handlerExecutor =
            ExecutorSupport.newHandlerExecutor("grpc-handler", ServerConfig.HANDLER_MAX_THREADS);

    public JanusGrpcServer(JanusServiceImpl service) {
        this.service = service;
        this.server = createServer();
    }

    private Server createServer() {
        int port = ServerConfig.GRPC_PORT;

        var chain = new ArrayList<ServerInterceptor>();
        chain.add(new HeaderServerInterceptor());
        var otelServer = OtelSupport.grpcServerInterceptor();
        if (otelServer != null) {
            chain.add(otelServer);
        }

        ServerServiceDefinition interceptedService =
                ServerInterceptors.intercept(service, chain.toArray(new ServerInterceptor[0]));

        log.info("Starting gRPC server on port {} (maxInbound={}B, flowWindow={}B, reflection={})",
                port, ServerConfig.GRPC_MAX_INBOUND_MSG, ServerConfig.GRPC_FLOW_WINDOW,
                ServerConfig.GRPC_REFLECTION_ENABLED);

        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(handlerExecutor)
                .addService(interceptedService)
                .addService(healthStatusManager.getHealthService())
                .maxInboundMessageSize(ServerConfig.GRPC_MAX_INBOUND_MSG)
                .flowControlWindow(ServerConfig.GRPC_FLOW_WINDOW)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .permitKeepAliveTime(5, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true);

        if (ServerConfig.GRPC_MAX_CONCURRENT_CALLS > 0) {
            builder.maxConcurrentCallsPerConnection(ServerConfig.GRPC_MAX_CONCURRENT_CALLS);
        }
        // Reflection is convenient for grpcurl in the demo but exposes the service
        // schema; make it opt-out for hardened deployments.
        if (ServerConfig.GRPC_REFLECTION_ENABLED) {
            builder.addService(ProtoReflectionServiceV1.newInstance());
        }
        // Opt-in TLS. Fail fast on misconfiguration rather than silently serving
        // plaintext.
        if (ServerConfig.tlsEnabled()) {
            try {
                builder.sslContext(TlsSupport.grpcServerSslContext());
            } catch (Exception e) {
                throw new IllegalStateException("gRPC server TLS setup failed", e);
            }
        }

        return builder.build();
    }

    public void start() throws Exception {
        server.start();
        healthStatusManager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        log.info("gRPC server started on port {}", ServerConfig.GRPC_PORT);
    }

    public void stop() {
        log.info("Shutting down gRPC server...");
        healthStatusManager.enterTerminalState();
        if (server != null) {
            try {
                server.shutdown();
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        handlerExecutor.shutdown();
        log.info("gRPC server stopped");
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public JanusServiceImpl getService() {
        return service;
    }
}
