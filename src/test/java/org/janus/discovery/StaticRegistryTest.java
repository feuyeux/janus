package org.janus.discovery;

import org.junit.jupiter.api.Test;
import org.janus.discovery.ServiceRegistry.ServiceInstance;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StaticRegistry} — the no-discovery registry that always
 * resolves a downstream service to a fixed set of {@code host:port} instances
 * (used to forward to an nginx reverse proxy in the load-balance experiments).
 */
class StaticRegistryTest {

    @Test
    void singleHostConstructorResolvesThatInstance() {
        StaticRegistry reg = new StaticRegistry("nginx", 8081, "ws");
        List<ServiceInstance> found = reg.discover("anything");
        assertEquals(1, found.size());
        assertEquals("nginx", found.get(0).host());
        assertEquals(8081, found.get(0).port());
        assertEquals("ws", found.get(0).protocol());
    }

    @Test
    void listConstructorPreservesAllInstancesAndOrder() {
        List<ServiceInstance> in = List.of(
                new ServiceInstance("lb-1", 8081, "ws"),
                new ServiceInstance("lb-2", 8082, "ws"),
                new ServiceInstance("lb-3", 8083, "ws"));
        StaticRegistry reg = new StaticRegistry(in);

        // discover ignores the service name and always returns the fixed set.
        List<ServiceInstance> found = reg.discover("ignored-name");
        assertEquals(in, found);
    }

    @Test
    void discoverIsIndependentOfServiceName() {
        StaticRegistry reg = new StaticRegistry("host", 9090, "grpc");
        assertEquals(reg.discover("svc-a"), reg.discover("svc-b"));
    }

    @Test
    void discoveredListIsImmutable() {
        StaticRegistry reg = new StaticRegistry("host", 9090, "grpc");
        List<ServiceInstance> found = reg.discover("svc");
        assertThrows(UnsupportedOperationException.class,
                () -> found.add(new ServiceInstance("x", 1, "ws")));
    }

    @Test
    void emptyInstanceListIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new StaticRegistry(List.of()));
    }

    @Test
    void nullInstanceListIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new StaticRegistry((List<ServiceInstance>) null));
    }

    @Test
    void registerAndDeregisterAreNoOpsAndDoNotAffectDiscovery() {
        StaticRegistry reg = new StaticRegistry("host", 8081, "ws");
        assertDoesNotThrow(() -> reg.register("svc", "other", 1234, "ws"));
        assertDoesNotThrow(() -> reg.deregister("svc"));
        // Still resolves the originally configured instance.
        assertEquals("host", reg.discover("svc").get(0).host());
    }

    @Test
    void closeIsANoOp() {
        StaticRegistry reg = new StaticRegistry("host", 8081, "ws");
        assertDoesNotThrow(reg::close);
    }
}
