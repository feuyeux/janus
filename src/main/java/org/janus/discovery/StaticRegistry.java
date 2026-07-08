package org.janus.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A no-discovery registry that always resolves a downstream service to a single,
 * statically-configured {@code host:port}.
 *
 * <p>Used when a node must forward to a <em>known address</em> rather than a
 * discovered instance list — e.g. forwarding to an nginx reverse proxy that
 * itself load-balances across the real backends:
 *
 * <pre>{@code
 *   client ──(ws json)──▶ front ──(ws binary)──▶ [nginx LB] ──▶ backend × N
 * }</pre>
 *
 * The forwarding client ({@link org.janus.ws.JanusWsClient}) opens its whole
 * connection pool to this one address; the reverse proxy then spreads those
 * connections across the real backends. {@link #register}/{@link #deregister}
 * are intentionally no-ops: a node using a static downstream does not itself
 * register through this instance.
 */
public class StaticRegistry implements ServiceRegistry {
    private static final Logger log = LoggerFactory.getLogger(StaticRegistry.class);

    private final List<ServiceInstance> instances;

    /** Single fixed downstream (e.g. one nginx). */
    public StaticRegistry(String host, int port, String protocol) {
        this(List.of(new ServiceInstance(host, port, protocol)));
    }

    /**
     * Multiple fixed downstreams (e.g. several nginx instances). The forwarding
     * pool is spread across them by {@link org.janus.ws.JanusWsClient}, letting a
     * front fan its connections out over a multi-instance load-balancer tier.
     */
    public StaticRegistry(List<ServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            throw new IllegalArgumentException("StaticRegistry requires at least one instance");
        }
        this.instances = List.copyOf(instances);
        if (this.instances.size() == 1) {
            ServiceInstance i = this.instances.get(0);
            log.info("Static downstream registry → {}:{} ({})", i.host(), i.port(), i.protocol());
        } else {
            log.info("Static downstream registry → {} instances {}", this.instances.size(), this.instances);
        }
    }

    @Override
    public void register(String serviceName, String host, int port, String protocol) {
        // no-op: a static-downstream node does not register through this registry
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) {
        return instances;
    }

    @Override
    public void deregister(String serviceName) {
        // no-op
    }

    @Override
    public void close() {
        // nothing to close
    }
}
