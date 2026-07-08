package org.janus.common;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the serial-mode overflow policies (P2-2). These test the
 * {@link ThreadPoolExecutor.RejectedExecutionHandler} constants directly against
 * a single-thread / tiny-queue executor (mirroring the serial handler executor),
 * without needing {@code JANUS_HANDLER_SERIAL} set in the environment.
 */
class SerialExecutorRejectionPolicyTest {

    /** Single worker, queue capacity 1 → the executor saturates after 2 tasks. */
    private ThreadPoolExecutor serialExecutor(java.util.concurrent.RejectedExecutionHandler policy) {
        return new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1), r -> {
                    Thread t = new Thread(r, "serial-test");
                    t.setDaemon(true);
                    return t;
                }, policy);
    }

    @Test
    void shedOnOverflowRejectsWhenSaturated() throws Exception {
        ThreadPoolExecutor exec = serialExecutor(ExecutorSupport.SHED_ON_OVERFLOW);
        CountDownLatch hold = new CountDownLatch(1);
        try {
            exec.execute(() -> await(hold));  // occupies the single worker
            exec.execute(() -> await(hold));  // fills the queue (cap 1)
            // Third task cannot be queued or run → rejected (caller sheds load).
            assertThrows(RejectedExecutionException.class, () -> exec.execute(() -> {}));
        } finally {
            hold.countDown();
            exec.shutdownNow();
        }
    }

    @Test
    void blockOnOverflowBlocksThenRunsTaskOnWorkerNotCaller() throws Exception {
        ThreadPoolExecutor exec = serialExecutor(ExecutorSupport.BLOCK_ON_OVERFLOW);
        CountDownLatch hold = new CountDownLatch(1);
        CountDownLatch cRan = new CountDownLatch(1);
        AtomicReference<String> cThread = new AtomicReference<>();
        try {
            exec.execute(() -> await(hold));  // occupies worker
            exec.execute(() -> await(hold));  // fills queue

            Thread mainThread = Thread.currentThread();
            CountDownLatch submitted = new CountDownLatch(1);
            Thread submitter = new Thread(() -> {
                // Should BLOCK here (queue full, worker busy) rather than reject.
                exec.execute(() -> {
                    cThread.set(Thread.currentThread().getName());
                    cRan.countDown();
                });
                submitted.countDown();
            }, "submitter");
            submitter.start();

            // While the executor is saturated the submit must not complete.
            assertFalse(submitted.await(300, TimeUnit.MILLISECONDS),
                    "BLOCK_ON_OVERFLOW must block the caller while saturated");

            // Free the worker: the two held tasks drain, a queue slot opens, and
            // the blocked submit then succeeds.
            hold.countDown();
            assertTrue(submitted.await(2, TimeUnit.SECONDS), "submit should unblock once space frees");
            assertTrue(cRan.await(2, TimeUnit.SECONDS), "the enqueued task must eventually run");

            // The task ran on the pool worker — never inlined onto the caller.
            assertEquals("serial-test", cThread.get());
            assertNotEquals(mainThread.getName(), cThread.get());
            submitter.join(2_000);
        } finally {
            hold.countDown();
            exec.shutdownNow();
        }
    }

    @Test
    void blockOnOverflowRejectsAfterShutdown() {
        ThreadPoolExecutor exec = serialExecutor(ExecutorSupport.BLOCK_ON_OVERFLOW);
        exec.shutdown();
        assertThrows(RejectedExecutionException.class, () -> exec.execute(() -> {}));
    }

    private static void await(CountDownLatch l) {
        try {
            l.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
