package org.janus.discovery;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

/**
 * gRPC {@link NameResolverProvider} backed by Nacos.
 *
 * <p>The {@link NamingService} (and its underlying connection, push subscription
 * and gRPC client) is a heavy, single-application resource: it owns long-lived
 * connections to the Nacos server and a thread pool for push events. Caching
 * one per provider instance — and registering the provider exactly once with
 * {@code NameResolverRegistry.getDefaultRegistry().register(provider)} — means
 * every gRPC channel that resolves {@code nacos://...} shares the same
 * connection pool and we do not leak clients.
 *
 * <p>Earlier drafts built a fresh {@code NamingService} on every
 * {@link #newNameResolver} call. gRPC re-resolves under load (channel
 * reconnect, subchannel re-creation on subchannel death, etc.), and each
 * call instantiated another NamingService. The Nacos gRPC client then
 * flooded the server with redundant connections and the resolver thread
 * burned CPU. Sharing one instance makes the resolver correct AND cheap.
 */
public class NacosNameResolverProvider extends NameResolverProvider {
    private static final Logger log = LoggerFactory.getLogger(NacosNameResolverProvider.class);

    protected static final String NACOS = "nacos";

    private final URI uri;
    // Shared NamingService for every newNameResolver() call. Created lazily on
    // the first resolve so a Nacos misconfiguration surfaces only when the
    // gRPC channel actually tries to resolve, not at provider registration.
    private volatile NamingService sharedNamingService;

    public NacosNameResolverProvider(URI targetUri) {
        this.uri = targetUri;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 6;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new NacosNameResolver(targetUri, buildNamingService());
    }

    @Override
    public String getDefaultScheme() {
        return NACOS;
    }

    private NamingService buildNamingService() {
        NamingService existing = sharedNamingService;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (sharedNamingService != null) {
                return sharedNamingService;
            }
            try {
                NamingService created = NacosFactory.createNamingService(buildNacosProperties(uri));
                sharedNamingService = created;
                log.info("Nacos NameResolverProvider sharing one NamingService for {}", uri);
                return created;
            } catch (NacosException e) {
                // Fail fast: returning null previously caused an NPE deep inside the
                // resolver on the first resolve. Surface the failure at channel setup.
                // The original NacosException often has a null getErrMsg() for early
                // connection errors — log the whole exception so the actual cause is
                // not lost (it shows up in the cause chain).
                log.error("build naming service error: errCode={} errMsg={} cause={}",
                        e.getErrCode(), e.getErrMsg(), e.getCause(), e);
                throw new IllegalStateException("Unable to create Nacos NamingService for " + uri, e);
            }
        }
    }

    private static Properties buildNacosProperties(URI uri) {
        Properties properties = new Properties();
        properties.put(com.alibaba.nacos.api.PropertyKeyConst.SERVER_ADDR,
                uri.getHost() + ":" + uri.getPort());
        return properties;
    }
}
