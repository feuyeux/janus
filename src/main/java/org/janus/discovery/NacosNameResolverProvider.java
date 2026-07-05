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

public class NacosNameResolverProvider extends NameResolverProvider {
    private static final Logger log = LoggerFactory.getLogger(NacosNameResolverProvider.class);

    protected static final String NACOS = "nacos";

    private final URI uri;

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
        try {
            return NacosFactory.createNamingService(buildNacosProperties(uri));
        } catch (NacosException e) {
            // Fail fast: returning null previously caused an NPE deep inside the
            // resolver on the first resolve. Surface the failure at channel setup.
            log.error("build naming service error: {}", e.getErrMsg());
            throw new IllegalStateException("Unable to create Nacos NamingService for " + uri, e);
        }
    }

    private static Properties buildNacosProperties(URI uri) {
        Properties properties = new Properties();
        properties.put(com.alibaba.nacos.api.PropertyKeyConst.SERVER_ADDR,
                uri.getHost() + ":" + uri.getPort());
        return properties;
    }
}
