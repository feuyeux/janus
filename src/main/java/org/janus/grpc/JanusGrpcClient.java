package org.janus.grpc;

import io.grpc.*;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.Constants;
import org.janus.config.ServerConfig;
import org.janus.discovery.EtcdNameResolverProvider;
import org.janus.discovery.NacosNameResolverProvider;
import org.janus.discovery.ServiceRegistry;
import org.janus.observability.OtelSupport;
import org.janus.proto.JanusServiceGrpc;
import org.janus.security.TlsSupport;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * gRPC client for connecting to downstream services via discovery.
 */
public class JanusGrpcClient {
    private static final Logger log = LoggerFactory.getLogger(JanusGrpcClient.class);

    private ManagedChannel channel;
    private JanusServiceGrpc.JanusServiceBlockingStub blockingStub;
    private JanusServiceGrpc.JanusServiceStub asyncStub;

    public void connect(ServiceRegistry registry) {
        try {
            NettyChannelBuilder builder;

            if (ServerConfig.isEtcdDiscovery()) {
                String target = "etcd:///" + ServerConfig.DOWNSTREAM_SERVICE;
                List<URI> endpoints = new ArrayList<>();
                endpoints.add(URI.create(normalizeEtcdEndpoint(ServerConfig.ETCD_ENDPOINT)));
                EtcdNameResolverProvider provider = EtcdNameResolverProvider.forEndpoints(endpoints);
                NameResolverRegistry.getDefaultRegistry().register(provider);
                builder = NettyChannelBuilder.forTarget(target)
                        .defaultLoadBalancingPolicy(Constants.LB_ROUND_ROBIN);
                log.info("gRPC client via etcd discovery: {}", target);
            } else if (ServerConfig.isNacosDiscovery()) {
                // Nacos 2.x and 3.x clients ship a gRPC-based NameResolver, but its
                // client SDK has to be carefully configured for the current Nacos
                // version (auth identity, server-status, push connection) and a
                // transient "Client not connected, current status:STARTING" error
                // leaves the gRPC channel with no addresses and a forever-hanging
                // request. The simpler and more robust path is to use the same
                // registry we already use for WS forwarding — NacosRegistry has
                // been verified to work and returns instances directly.
                List<ServiceRegistry.ServiceInstance> instances =
                        registry.discover(ServerConfig.DOWNSTREAM_SERVICE);
                if (instances.isEmpty()) {
                    log.error("No downstream instances discovered via Nacos registry; "
                            + "the gRPC channel will not have a peer to connect to");
                    return;
                }
                ServiceRegistry.ServiceInstance inst = instances.get(0);
                builder = NettyChannelBuilder.forAddress(inst.host(), inst.port());
                log.info("gRPC client via Nacos discovery: {}:{}",
                        inst.host(), inst.port());
            } else {
                // Direct connection using discovered instances
                List<ServiceRegistry.ServiceInstance> instances =
                        registry.discover(ServerConfig.DOWNSTREAM_SERVICE);
                if (instances.isEmpty()) {
                    log.error("No downstream instances discovered");
                    return;
                }
                ServiceRegistry.ServiceInstance inst = instances.get(0);
                builder = NettyChannelBuilder.forAddress(inst.host(), inst.port());
                log.info("gRPC client direct: {}:{}", inst.host(), inst.port());
            }

            builder.keepAliveTime(10, java.util.concurrent.TimeUnit.SECONDS)
                    .keepAliveTimeout(1, java.util.concurrent.TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .maxInboundMessageSize(ServerConfig.GRPC_MAX_INBOUND_MSG)
                    .enableRetry();

            // Opt-in TLS; default remains plaintext for the demo stack.
            if (ServerConfig.tlsEnabled()) {
                builder.sslContext(TlsSupport.grpcClientSslContext())
                        .negotiationType(NegotiationType.TLS);
                log.info("gRPC client using TLS");
            } else {
                builder.negotiationType(NegotiationType.PLAINTEXT);
            }
            channel = builder.build();

            var chain = new ArrayList<ClientInterceptor>();
            chain.add(new HeaderClientInterceptor());
            var otelClient = OtelSupport.grpcClientInterceptor();
            if (otelClient != null) {
                chain.add(otelClient);
            }

            Channel interceptChannel = ClientInterceptors.intercept(
                    channel, chain.toArray(new ClientInterceptor[0]));

            blockingStub = JanusServiceGrpc.newBlockingStub(interceptChannel);
            asyncStub = JanusServiceGrpc.newStub(interceptChannel);

            log.info("gRPC client connected to downstream");
        } catch (Exception e) {
            log.error("Failed to connect gRPC client", e);
        }
    }

    private static String normalizeEtcdEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) return "http://localhost:2379";
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            return "http://" + endpoint;
        }
        return endpoint;
    }

    public JanusServiceGrpc.JanusServiceBlockingStub getBlockingStub() {
        return blockingStub;
    }

    public JanusServiceGrpc.JanusServiceStub getAsyncStub() {
        return asyncStub;
    }

    public void shutdown() {
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isConnected() {
        return channel != null && !channel.isShutdown();
    }
}
