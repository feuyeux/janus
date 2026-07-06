package org.janus.discovery;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NacosRegistry implements ServiceRegistry {
    private static final Logger log = LoggerFactory.getLogger(NacosRegistry.class);

    private final NamingService namingService;
    private final String endpoint;
    // Remember what we actually registered so deregister targets the same instance
    // instead of assuming the WS port.
    private volatile String registeredHost;
    private volatile int registeredPort = -1;

    public NacosRegistry(String endpoint) {
        this.endpoint = endpoint;
        try {
            Properties properties = new Properties();
            properties.put(com.alibaba.nacos.api.PropertyKeyConst.SERVER_ADDR, endpoint);
            this.namingService = NacosFactory.createNamingService(properties);
            log.info("Nacos registry connected: {}", endpoint);
        } catch (NacosException e) {
            throw new RuntimeException("Failed to create Nacos naming service: " + e.getMessage(), e);
        }
    }

    @Override
    public void register(String serviceName, String host, int port, String protocol) {
        try {
            Instance instance = new Instance();
            instance.setIp(host);
            instance.setPort(port);
            instance.getMetadata().put("protocol", protocol);
            namingService.registerInstance(serviceName, instance);
            registeredHost = host;
            registeredPort = port;
            log.info("Registered [{}] in Nacos: {}:{} protocol={}", serviceName, host, port, protocol);
        } catch (NacosException e) {
            log.error("Failed to register in Nacos: {}", e.getMessage());
        }
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) {
        try {
            // selectInstances(name, healthy=true) returns only healthy & enabled
            // instances, so we never hand a dead node to the load balancer.
            List<Instance> instances = namingService.selectInstances(serviceName, true);
            List<ServiceInstance> result = new ArrayList<>();
            for (Instance inst : instances) {
                String protocol = inst.getMetadata().getOrDefault("protocol", "ws");
                result.add(new ServiceInstance(inst.getIp(), inst.getPort(), protocol));
            }
            log.debug("Discovered {} healthy instances for [{}] in Nacos", result.size(), serviceName);
            return result;
        } catch (NacosException e) {
            log.error("Failed to discover in Nacos: {}", e.getMessage());
            return List.of();
        }
    }

    @Override
    public void deregister(String serviceName) {
        if (registeredPort < 0 || registeredHost == null) {
            log.info("Nothing registered in Nacos to deregister for [{}]", serviceName);
            return;
        }
        try {
            namingService.deregisterInstance(serviceName, registeredHost, registeredPort);
            log.info("Deregistered [{}] from Nacos: {}:{}", serviceName, registeredHost, registeredPort);
        } catch (NacosException e) {
            log.error("Failed to deregister from Nacos: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        // Nacos client doesn't need explicit close
        log.info("Nacos registry closed: {}", endpoint);
    }
}
