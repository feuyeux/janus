package org.janus.common;

import org.janus.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
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

    /** Bounded backlog for the serial executor before its overflow policy kicks in. */
    private static final int SERIAL_QUEUE_CAPACITY = 1_000;

    /**
     * Serial-mode overflow policy for transports that can cheaply signal
     * "server busy" back to the client: log and reject with
     * {@link RejectedExecutionException} so the caller sheds load. The WS server
     * catches this and replies with a 503-style error frame. Work is never
     * inlined onto the caller (I/O) thread.
     */
    public static final RejectedExecutionHandler SHED_ON_OVERFLOW = (r, exec) -> {
        log.warn("Serial handler queue full ({} queued); shedding task", exec.getQueue().size());
        throw new RejectedExecutionException("serial handler queue full");
    };

    /**
     * Serial-mode overflow policy that applies <em>backpressure</em> instead of
     * rejecting: the submitting thread blocks until the single worker frees a
     * queue slot. Used by the gRPC path, where a raw rejection from the server
     * call executor surfaces to the client as an opaque stream reset rather than
     * a clean status; blocking the transport thread instead propagates HTTP/2
     * flow-control backpressure to the client. Crucially it enqueues the task
     * (via {@link LinkedBlockingQueue#put}) rather than running it on the caller,
     * so the single worker still processes requests strictly one at a time.
     */
    public static final RejectedExecutionHandler BLOCK_ON_OVERFLOW = (r, exec) -> {
        if (exec.isShutdown()) {
            throw new RejectedExecutionException("serial handler shut down");
        }
        try {
            exec.getQueue().put(r);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("interrupted while enqueuing serial task", e);
        }
    };

    /**
     * Create an executor for request handling, using {@link #SHED_ON_OVERFLOW}
     * as the serial-mode overflow policy (suitable for the WS server).
     *
     * @param name     thread-name prefix (also used for the virtual-thread name)
     * @param maxPlatformThreads upper bound for the platform-thread fallback pool;
     *                           ignored when virtual threads are available
     */
    public static ExecutorService newHandlerExecutor(String name, int maxPlatformThreads) {
        return newHandlerExecutor(name, maxPlatformThreads, SHED_ON_OVERFLOW);
    }

    /**
     * Create an executor for request handling with an explicit serial-mode
     * overflow policy.
     *
     * <p>The {@code serialOverflowPolicy} only takes effect when
     * {@code JANUS_HANDLER_SERIAL=Y}; the default (non-serial) virtual-thread /
     * bounded platform-thread paths are unaffected. Transports differ in how a
     * saturated serial backend should degrade: WS uses {@link #SHED_ON_OVERFLOW}
     * (clean 503), gRPC uses {@link #BLOCK_ON_OVERFLOW} (transport backpressure)
     * because a rejected server-call task would otherwise become an opaque
     * stream reset.
     *
     * @param name     thread-name prefix (also used for the virtual-thread name)
     * @param maxPlatformThreads upper bound for the platform-thread fallback pool;
     *                           ignored when virtual threads are available
     * @param serialOverflowPolicy overflow handler for the serial (single-thread)
     *                             executor; ignored outside serial mode
     */
    public static ExecutorService newHandlerExecutor(String name, int maxPlatformThreads,
            RejectedExecutionHandler serialOverflowPolicy) {
        // Serial mode: a single worker thread processes requests strictly one at a
        // time. Chosen explicitly (JANUS_HANDLER_SERIAL=Y) to model a serial backend
        // for load-balancer experiments; overrides the virtual-thread path so the
        // one-at-a-time guarantee holds even on JDK 21+. A bounded queue absorbs a
        // small backlog; on overflow the supplied policy decides whether to shed
        // (WS → 503) or apply backpressure (gRPC → block the transport thread).
        if (ServerConfig.HANDLER_SERIAL) {
            String mode = serialOverflowPolicy == BLOCK_ON_OVERFLOW ? "block/backpressure" : "shed";
            log.info("Serial handler executor for [{}] (single-threaded, one request at a time, overflow={})",
                    name, mode);
            ThreadPoolExecutor serial = new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(SERIAL_QUEUE_CAPACITY), namedFactory(name),
                    serialOverflowPolicy);
            return serial;
        }
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
