package org.janus.discovery;

import io.etcd.jetcd.*;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.config.ServerConfig;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EtcdRegistry implements ServiceRegistry {
    private static final Logger log = LoggerFactory.getLogger(EtcdRegistry.class);

    private final Client etcd;
    private final String endpoint;
    private final AtomicLong leaseId = new AtomicLong(0);
    private volatile boolean registered = false;
    private String registeredKey;

    public EtcdRegistry(String endpoint) {
        this.endpoint = normalizeEndpoint(endpoint);
        this.etcd = Client.builder().endpoints(URI.create(this.endpoint)).build();
        log.info("etcd registry connected: {}", this.endpoint);
    }

    private static String normalizeEndpoint(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) {
            return "http://localhost:2379";
        }
        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            return "http://" + endpoint;
        }
        return endpoint;
    }

    @Override
    public void register(String serviceName, String host, int port, String protocol) {
        try {
            long lease = etcd.getLeaseClient().grant(ServerConfig.ETCD_TTL).get().getID();
            leaseId.set(lease);

            String uri = "grpc://" + host + ":" + port;
            registeredKey = serviceName + "/" + uri;
            ByteSequence key = ByteSequence.from(registeredKey, StandardCharsets.US_ASCII);
            ByteSequence value = ByteSequence.from(protocol + "|" + lease, StandardCharsets.US_ASCII);
            PutOption option = PutOption.builder().withLeaseId(lease).build();
            etcd.getKVClient().put(key, value, option);

            // Keep alive
            etcd.getLeaseClient().keepAlive(lease, new StreamObserver<>() {
                @Override
                public void onNext(LeaseKeepAliveResponse response) {
                    log.debug("etcd lease renewed: {}", response.getID());
                }

                @Override
                public void onError(Throwable t) {
                    log.error("etcd lease keep-alive error", t);
                }

                @Override
                public void onCompleted() {
                    log.info("etcd lease completed");
                }
            });

            registered = true;
            log.info("Registered [{}] in etcd: {}:{} protocol={}", serviceName, host, port, protocol);
        } catch (Exception e) {
            log.error("Failed to register in etcd", e);
        }
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) {
        List<ServiceInstance> result = new ArrayList<>();
        try {
            ByteSequence prefix = ByteSequence.from(serviceName, StandardCharsets.UTF_8);
            GetOption option = GetOption.builder().isPrefix(true).build();
            GetResponse response = etcd.getKVClient().get(prefix, option).get();

            for (KeyValue kv : response.getKvs()) {
                String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
                // Extract URI from key: serviceName/grpc://host:port
                String uriStr = keyStr.substring(keyStr.indexOf("/") + 1);
                URI uri = URI.create(uriStr);
                String valStr = kv.getValue().toString(StandardCharsets.US_ASCII);
                String protocol = valStr.contains("|") ? valStr.split("\\|")[0] : "grpc";
                result.add(new ServiceInstance(uri.getHost(), uri.getPort(), protocol));
            }
            log.debug("Discovered {} instances for [{}] in etcd", result.size(), serviceName);
        } catch (Exception e) {
            log.error("Failed to discover in etcd", e);
        }
        return result;
    }

    @Override
    public void deregister(String serviceName) {
        if (registered && registeredKey != null) {
            try {
                etcd.getKVClient().delete(ByteSequence.from(registeredKey, StandardCharsets.US_ASCII)).get();
                log.info("Deregistered [{}] from etcd", serviceName);
            } catch (Exception e) {
                log.error("Failed to deregister from etcd", e);
            }
        }
    }

    @Override
    public void close() {
        try {
            etcd.close();
            log.info("etcd registry closed: {}", endpoint);
        } catch (Exception e) {
            log.error("Error closing etcd client", e);
        }
    }
}
