package org.janus.discovery;

import java.util.List;

/**
 * Service registry interface for registration and discovery.
 */
public interface ServiceRegistry extends AutoCloseable {

    /**
     * Register this server instance.
     */
    void register(String serviceName, String host, int port, String protocol);

    /**
     * Discover service instances.
     */
    List<ServiceInstance> discover(String serviceName);

    /**
     * Deregister this server instance.
     */
    void deregister(String serviceName);

    record ServiceInstance(String host, int port, String protocol) {}
}
