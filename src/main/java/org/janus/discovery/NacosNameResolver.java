package org.janus.discovery;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

/**
 * gRPC {@link NameResolver} backed by Nacos.
 *
 * <p>Unlike a one-shot resolver, this subscribes to Nacos so instance up/down
 * events are pushed and the gRPC load balancer's address set stays current
 * without a restart. Only healthy+enabled instances are surfaced.
 */
public class NacosNameResolver extends NameResolver {
    private static final Logger log = LoggerFactory.getLogger(NacosNameResolver.class);

    private final String serviceName;
    private final NamingService namingService;
    private volatile Listener listener;
    private volatile EventListener subscription;
    // Last non-empty resolved address set, kept so a transient empty result does
    // not clear the load balancer.
    private volatile List<EquivalentAddressGroup> lastGood = List.of();
    // Background retry thread that re-attempts the initial selectInstances() /
    // subscribe() until the NamingService becomes usable. Nacos's gRPC client
    // is itself async — on a cold start, the first selectInstances() can fail
    // with "Client not connected, current status:STARTING" before the SDK has
    // established its connection. Without this loop, the gRPC channel would
    // never receive an onAddresses() callback and requests would hang until
    // the application restarted.
    private Thread bootstrapRetry;
    private volatile boolean bootstrapped = false;

    public NacosNameResolver(URI targetUri, NamingService namingService) {
        this.serviceName = targetUri.getAuthority();
        this.namingService = namingService;
    }

    @Override
    public String getServiceAuthority() {
        return serviceName;
    }

    @Override
    public void start(Listener listener) {
        // gRPC may call start() more than once in some rebind paths; a second
        // call must not re-subscribe to Nacos (which would deliver each event
        // twice to update()) and must not flip the address set twice.
        if (this.listener != null) {
            this.listener.onAddresses(lastGood, Attributes.EMPTY);
            return;
        }
        this.listener = listener;
        if (namingService == null) {
            log.error("Nacos NamingService unavailable; cannot resolve [{}]", serviceName);
            return;
        }
        update();
        // Push-based refresh so membership changes are reflected without restart.
        try {
            this.subscription = event -> update();
            namingService.subscribe(serviceName, subscription);
        } catch (NacosException e) {
            log.error("Nacos subscribe failed for [{}]: {}", serviceName, e.getErrMsg());
        }
        // If the initial update() and subscribe() both failed (the typical
        // cold-start case where the Nacos SDK is still in STARTING state),
        // spin up a single background retry thread that re-attempts until
        // either a non-empty address set has been delivered, or the
        // NamingService reaches 30 seconds without becoming usable. The
        // thread exits as soon as bootstrapped=true; it never holds a lock.
        if (!bootstrapped) {
            startBootstrapRetry();
        }
    }

    private void startBootstrapRetry() {
        Thread t = new Thread(() -> {
            long deadline = System.currentTimeMillis() + 30_000L;
            while (!bootstrapped && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    namingService.subscribe(serviceName, event -> update());
                    // subscribe() succeeded → NamingService is reachable; now
                    // selectInstances should also work, or it will be retried
                    // via the push listener on the next server push.
                    bootstrapped = true;
                    update();
                } catch (NacosException e) {
                    // NamingService still STARTING (or the server briefly
                    // unreachable). Keep retrying until deadline.
                }
            }
        }, "nacos-resolver-bootstrap-" + serviceName);
        t.setDaemon(true);
        t.start();
        bootstrapRetry = t;
    }

    @Override
    public void refresh() {
        update();
    }

    @Override
    public void shutdown() {
        if (namingService != null && subscription != null) {
            try {
                namingService.unsubscribe(serviceName, subscription);
            } catch (NacosException e) {
                log.warn("Nacos unsubscribe failed for [{}]: {}", serviceName, e.getErrMsg());
            }
        }
    }

    private void update() {
        Listener current = this.listener;
        if (current == null || namingService == null) {
            return;
        }
        try {
            // healthy=true → only healthy & enabled instances reach the balancer.
            List<Instance> instances = namingService.selectInstances(serviceName, true);
            List<EquivalentAddressGroup> groups = instances.stream()
                    .map(instance -> new EquivalentAddressGroup(
                            new InetSocketAddress(instance.getIp(), instance.getPort())))
                    .collect(Collectors.toList());

            if (groups.isEmpty()) {
                // Transient empties happen right after a peer (re)registers, before
                // its heartbeat marks it healthy. Don't clear the balancer's address
                // set on such a blip — keep the last known-good set so in-flight and
                // new RPCs still have a target. A genuine full outage is surfaced by
                // the load balancer's own connectivity handling.
                log.warn("no healthy instances for [{}] in Nacos; retaining last-good set ({} addr)",
                        serviceName, lastGood.size());
                return;
            }
            lastGood = groups;
            bootstrapped = true;
            log.debug("Resolved {} healthy instances for [{}] from Nacos", groups.size(), serviceName);
            current.onAddresses(groups, Attributes.EMPTY);
        } catch (NacosException e) {
            log.error("Nacos discovery error: {}", e.getErrMsg());
        }
    }
}
