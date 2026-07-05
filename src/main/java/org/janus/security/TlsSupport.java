package org.janus.security;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janus.config.ServerConfig;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * Opt-in TLS scaffolding. Disabled unless {@code JANUS_TLS_ENABLED=Y}, so the
 * default plaintext demo stack and the test suite are unaffected.
 *
 * <ul>
 *   <li><b>gRPC</b> uses PEM material via Netty/{@link GrpcSslContexts}
 *       ({@code JANUS_TLS_CERT}, {@code JANUS_TLS_KEY}, {@code JANUS_TLS_CA}).</li>
 *   <li><b>WebSocket (wss)</b> uses a Java keystore/truststore to build a javax
 *       {@link SSLContext} ({@code JANUS_TLS_KEYSTORE} + password, optional
 *       {@code JANUS_TLS_TRUSTSTORE}).</li>
 * </ul>
 *
 * <p>Setup fails fast (throws) rather than silently downgrading to plaintext,
 * so a TLS misconfiguration cannot leave a node unexpectedly unencrypted.
 */
public final class TlsSupport {
    private static final Logger log = LoggerFactory.getLogger(TlsSupport.class);

    private TlsSupport() {}

    // ─── gRPC (Netty, PEM) ─────────────────────────────────────────────────────

    /** Build the server-side gRPC {@link SslContext} (optionally requiring mTLS). */
    public static SslContext grpcServerSslContext() throws SSLException {
        File cert = requireFile(ServerConfig.TLS_CERT, "JANUS_TLS_CERT");
        File key = requireFile(ServerConfig.TLS_KEY, "JANUS_TLS_KEY");
        SslContextBuilder builder = GrpcSslContexts.forServer(cert, key);
        if (!ServerConfig.TLS_CA.isEmpty()) {
            builder.trustManager(requireFile(ServerConfig.TLS_CA, "JANUS_TLS_CA"));
        }
        builder.clientAuth(ServerConfig.TLS_MTLS ? ClientAuth.REQUIRE : ClientAuth.NONE);
        log.info("gRPC server TLS enabled (mTLS={})", ServerConfig.TLS_MTLS);
        return builder.build();
    }

    /** Build the client-side gRPC {@link SslContext} (presents a cert when mTLS). */
    public static SslContext grpcClientSslContext() throws SSLException {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        if (!ServerConfig.TLS_CA.isEmpty()) {
            builder.trustManager(requireFile(ServerConfig.TLS_CA, "JANUS_TLS_CA"));
        }
        if (ServerConfig.TLS_MTLS) {
            builder.keyManager(
                    requireFile(ServerConfig.TLS_CERT, "JANUS_TLS_CERT"),
                    requireFile(ServerConfig.TLS_KEY, "JANUS_TLS_KEY"));
        }
        log.info("gRPC client TLS enabled (mTLS={})", ServerConfig.TLS_MTLS);
        return builder.build();
    }

    // ─── WebSocket (javax SSLContext from keystore) ─────────────────────────────

    public static SSLContext wsServerSslContext() throws Exception {
        return keystoreSslContext();
    }

    public static SSLSocketFactory wsClientSocketFactory() throws Exception {
        return keystoreSslContext().getSocketFactory();
    }

    private static SSLContext keystoreSslContext() throws Exception {
        char[] pw = ServerConfig.TLS_KEYSTORE_PASSWORD.toCharArray();
        KeyStore ks = KeyStore.getInstance(ServerConfig.TLS_KEYSTORE_TYPE);
        try (InputStream in = new FileInputStream(
                requireFile(ServerConfig.TLS_KEYSTORE, "JANUS_TLS_KEYSTORE"))) {
            ks.load(in, pw);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (!ServerConfig.TLS_TRUSTSTORE.isEmpty()) {
            KeyStore ts = KeyStore.getInstance(ServerConfig.TLS_KEYSTORE_TYPE);
            try (InputStream in = new FileInputStream(ServerConfig.TLS_TRUSTSTORE)) {
                ts.load(in, ServerConfig.TLS_TRUSTSTORE_PASSWORD.toCharArray());
            }
            tmf.init(ts);
        } else {
            tmf.init((KeyStore) null); // JVM default trust store
        }

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        log.info("WS TLS SSLContext initialised (keystore type={})", ServerConfig.TLS_KEYSTORE_TYPE);
        return ctx;
    }

    private static File requireFile(String path, String name) {
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException(name + " is required when TLS is enabled");
        }
        File f = new File(path);
        if (!f.isFile()) {
            throw new IllegalStateException(name + " not found: " + path);
        }
        return f;
    }
}
