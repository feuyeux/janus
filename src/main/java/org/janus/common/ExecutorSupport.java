package org.janus.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds executors tuned for high concurrency / high throughput / low latency.
 *
 * <p>On JDK 21+ (the production runtime is JDK 25) this returns a
 * virtual-thread-per-task executor, which scales to a very large number of
 * concurrent blocking tasks (WS handling, blocking gRPC forwarding) with
 * minimal memory and low scheduling latency. On older JDKs — including the
 * JDK 17 used for local compilation — it falls back to an elastic cached
 * platform-thread pool so the code compiles and runs everywhere.
 *
 * <p>Virtual threads are looked up reflectively on purpose: it lets the source
 * stay compilable at language level 17 while still using the JDK 21+ API when
 * present at runtime.
 */
public final class ExecutorSupport {
    private static final Logger log = LoggerFactory.getLogger(ExecutorSupport.class);

    private ExecutorSupport() {}

    /**
     * Create an executor for request handling.
     *
     * @param name     thread-name prefix (also used for the virtual-thread name)
     * @param maxPlatformThreads upper bound for the platform-thread fallback pool;
     *                           ignored when virtual threads are available
     */
    public static ExecutorService newHandlerExecutor(String name, int maxPlatformThreads) {
        ExecutorService vt = tryVirtualThreadExecutor();
        if (vt != null) {
            log.info("Using virtual-thread executor for [{}]", name);
            return vt;
        }
        int max = maxPlatformThreads > 0 ? maxPlatformThreads : 256;
        log.info("Virtual threads unavailable; using bounded platform-thread pool for [{}] (max={})", name, max);
        ThreadFactory tf = namedFactory(name);
        // Bounded, elastic pool. A large bounded queue absorbs bursts; on true
        // saturation the AbortPolicy rejects (RejectedExecutionException) so the
        // caller can shed load gracefully — crucially, work is NEVER inlined onto
        // the WS/gRPC I/O thread (which CallerRunsPolicy would do), keeping the
        // transport responsive. This path only applies on JDK < 21; the JDK 21+
        // runtime uses virtual threads above.
        int core = Math.min(32, max);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                core, max, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10_000), tf,
                new ThreadPoolExecutor.AbortPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private static ExecutorService tryVirtualThreadExecutor() {
        try {
            Method m = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) m.invoke(null);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private static ThreadFactory namedFactory(String name) {
        AtomicLong seq = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, name + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
