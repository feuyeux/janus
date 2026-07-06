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
            log.debug("Resolved {} healthy instances for [{}] from Nacos", groups.size(), serviceName);
            current.onAddresses(groups, Attributes.EMPTY);
        } catch (NacosException e) {
            log.error("Nacos discovery error: {}", e.getErrMsg());
        }
    }
}
