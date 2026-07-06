package org.janus.discovery;

import com.google.common.base.Preconditions;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.annotation.concurrent.GuardedBy;

public class EtcdNameResolver extends NameResolver implements Watch.Listener {
    private static final Logger log = LoggerFactory.getLogger(EtcdNameResolver.class);

    private final Client etcd;
    private final String serviceDir;
    @GuardedBy("this")
    private final Set<URI> serviceUris;

    @GuardedBy("this")
    private Listener listener;

    EtcdNameResolver(List<URI> endpoints, String serviceDir) {
        this.etcd = Client.builder().endpoints(endpoints).build();
        this.serviceDir = serviceDir;
        this.serviceUris = new HashSet<>();
    }

    @Override
    public String getServiceAuthority() {
        return serviceDir;
    }

    @Override
    public void start(Listener listener) {
        synchronized (this) {
            Preconditions.checkState(this.listener == null, "already started");
            this.listener = Preconditions.checkNotNull(listener, "listener");
        }
        initializeAndWatch();
    }

    @Override
    public void shutdown() {
        etcd.close();
    }

    @Override
    public void onNext(WatchResponse watchResponse) {
        synchronized (this) {
            for (WatchEvent event : watchResponse.getEvents()) {
                String svcAddress;
                switch (event.getEventType()) {
                    case PUT:
                        // Watch events must strip the service-dir prefix exactly
                        // like the initial load; otherwise the raw key
                        // "svc/grpc://host:port" parses to a URI with a null host.
                        svcAddress = getUriFromDir(event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
                        try {
                            URI uri = new URI(svcAddress);
                            serviceUris.add(uri);
                        } catch (URISyntaxException e) {
                            log.warn("ignoring invalid uri: {}", svcAddress, e);
                        }
                        break;
                    case DELETE:
                        // Watch events must strip the service-dir prefix exactly
                        // like the initial load; otherwise the raw key
                        // "svc/grpc://host:port" parses to a URI with a null host.
                        svcAddress = getUriFromDir(event.getKeyValue().getKey().toString(StandardCharsets.UTF_8));
                        try {
                            URI uri = new URI(svcAddress);
                            serviceUris.remove(uri);
                        } catch (URISyntaxException e) {
                            log.warn("ignoring invalid uri: {}", svcAddress, e);
                        }
                        break;
                    case UNRECOGNIZED:
                }
            }
            updateListener();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("etcd watcher error", throwable);
    }

    @Override
    public void onCompleted() {}

    private void initializeAndWatch() {
        ByteSequence prefix = ByteSequence.from(serviceDir, StandardCharsets.UTF_8);
        GetOption option = GetOption.builder().isPrefix(true).build();

        GetResponse query;
        try (KV kv = etcd.getKVClient()) {
            query = kv.get(prefix, option).get();
        } catch (Exception e) {
            throw new RuntimeException("Unable to contact etcd", e);
        }

        synchronized (this) {
            for (KeyValue kv : query.getKvs()) {
                String svcAddress = getUriFromDir(kv.getKey().toString(StandardCharsets.UTF_8));
                try {
                    URI uri = new URI(svcAddress);
                    serviceUris.add(uri);
                } catch (URISyntaxException e) {
                    log.warn("Unable to parse server address: {}", svcAddress, e);
                }
            }

            updateListener();
        }

        WatchOption options = WatchOption.builder().withRevision(query.getHeader().getRevision()).build();
        etcd.getWatchClient().watch(prefix, options, this);
    }

    // Callers must hold the monitor on `this`; serviceUris and listener are both
    // guarded by it.
    @GuardedBy("this")
    private void updateListener() {
        log.debug("updating server list from etcd...");
        List<EquivalentAddressGroup> svcAddressList = new ArrayList<>();
        for (URI uri : serviceUris) {
            log.debug("online: {}", uri);
            List<SocketAddress> socketAddresses = new ArrayList<>();
            socketAddresses.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
            svcAddressList.add(new EquivalentAddressGroup(socketAddresses));
        }
        if (svcAddressList.isEmpty()) {
            log.warn("no servers online for: {}", serviceDir);
        } else if (listener != null) {
            listener.onAddresses(svcAddressList, Attributes.EMPTY);
        }
    }

    private static String getUriFromDir(String dir) {
        String tmp = dir.replace("://", "~");
        String[] tmps = tmp.split("/");
        return tmps[tmps.length - 1].replace("~", "://");
    }
}
