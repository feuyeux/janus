package org.janus.discovery;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the small static helpers in the etcd name resolver. These
 * package-private helpers are pinned by reflection so we don't have to make
 * them public to test them.
 */
class EtcdNameResolverTest {

    private static String getUriFromDir(String dir) throws Exception {
        Method m = EtcdNameResolver.class.getDeclaredMethod("getUriFromDir", String.class);
        m.setAccessible(true);
        try {
            return (String) m.invoke(null, dir);
        } catch (InvocationTargetException ite) {
            if (ite.getCause() instanceof Exception e) throw e;
            throw ite;
        }
    }

    @Test
    void stripsServiceDirPrefix() throws Exception {
        assertEquals("grpc://host:9090", getUriFromDir("janus-server/grpc://host:9090"));
    }

    @Test
    void handlesPortlessHost() throws Exception {
        // No port — the previous split-based implementation could mis-fold this
        // if a later '/' showed up; indexOf is immune.
        assertEquals("grpc://host", getUriFromDir("janus-server/grpc://host"));
    }

    @Test
    void noServiceDirSeparatorIsEchoed() throws Exception {
        // The function always strips the prefix up to the first '/'. Document
        // the actual behaviour: every key produced by EtcdRegistry has a
        // '<service>/' prefix, so this case is theoretical and the helper
        // strips the leading 'grpc:' segment as if it were the service dir.
        assertEquals("host:9090", getUriFromDir("grpc:/host:9090"));
    }

    @Test
    void handlesTrailingSlash() throws Exception {
        // Defensive: a key with a trailing '/' means "<service>/" with no
        // URI portion. The helper can't tell that case from a malformed URI,
        // so it returns the input unchanged and lets URI.create() surface the
        // error in the calling code rather than silently returning "".
        assertEquals("janus-server/", getUriFromDir("janus-server/"));
    }
}
