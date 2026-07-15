import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class JanusWsLoad {
    private static final String DEFAULT_URL = "ws://127.0.0.1:8080/json";

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        LongAdder success = new LongAdder();
        LongAdder failure = new LongAdder();
        CountDownLatch latch = new CountDownLatch(config.parallelism);
        List<Thread> workers = new ArrayList<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(config.durationSeconds);

        for (int i = 0; i < config.parallelism; i++) {
            int workerId = i;
            Thread worker = new Thread(() -> {
                try {
                    runWorker(config, workerId, deadlineNanos, success, failure);
                } finally {
                    latch.countDown();
                }
            }, "janus-ws-load-" + i);
            worker.start();
            workers.add(worker);
        }

        latch.await();
        System.out.println("Janus WS load finished");
        System.out.println("  url=" + config.url);
        System.out.println("  durationSeconds=" + config.durationSeconds);
        System.out.println("  parallelism=" + config.parallelism);
        System.out.println("  success=" + success.sum());
        System.out.println("  failure=" + failure.sum());
        if (failure.sum() > 0) {
            System.exit(2);
        }
    }

    private static void runWorker(Config config, int workerId, long deadlineNanos, LongAdder success, LongAdder failure) {
        WsSession session = null;
        try {
            session = WsSession.connect(config.url, config.connectTimeoutSeconds);
            int seq = 0;
            while (System.nanoTime() < deadlineNanos) {
                String payload = "{\"method\":\"TALK\",\"mode\":\"REQUEST\",\"data\":\"" + (seq % 6)
                        + "\",\"meta\":\"arthas-load-" + workerId + "\",\"seq\":" + seq
                        + ",\"stream_end\":true}";
                session.sendText(payload);
                String response = session.awaitText(config.responseTimeoutSeconds);
                if (response.contains("\"status\":200")) {
                    success.increment();
                } else {
                    failure.increment();
                    System.err.println("Unexpected response: " + response);
                }
                seq++;
                if (config.pauseMillis > 0) {
                    Thread.sleep(config.pauseMillis);
                }
            }
        } catch (Exception e) {
            failure.increment();
            System.err.println("Worker " + workerId + " failed: " + e.getMessage());
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    private static final class Config {
        final String url;
        final int durationSeconds;
        final int parallelism;
        final int pauseMillis;
        final int connectTimeoutSeconds;
        final int responseTimeoutSeconds;

        private Config(String url, int durationSeconds, int parallelism, int pauseMillis,
                       int connectTimeoutSeconds, int responseTimeoutSeconds) {
            this.url = url;
            this.durationSeconds = durationSeconds;
            this.parallelism = parallelism;
            this.pauseMillis = pauseMillis;
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.responseTimeoutSeconds = responseTimeoutSeconds;
        }

        static Config parse(String[] args) {
            String url = DEFAULT_URL;
            int durationSeconds = 70;
            int parallelism = 4;
            int pauseMillis = 0;
            int connectTimeoutSeconds = 10;
            int responseTimeoutSeconds = 10;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--url" -> url = requireValue(arg, args, ++i);
                    case "--duration-seconds" -> durationSeconds = Integer.parseInt(requireValue(arg, args, ++i));
                    case "--parallelism" -> parallelism = Integer.parseInt(requireValue(arg, args, ++i));
                    case "--pause-millis" -> pauseMillis = Integer.parseInt(requireValue(arg, args, ++i));
                    case "--connect-timeout-seconds" -> connectTimeoutSeconds = Integer.parseInt(requireValue(arg, args, ++i));
                    case "--response-timeout-seconds" -> responseTimeoutSeconds = Integer.parseInt(requireValue(arg, args, ++i));
                    case "-h", "--help" -> {
                        printUsage();
                        System.exit(0);
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            if (durationSeconds <= 0 || parallelism <= 0 || pauseMillis < 0) {
                throw new IllegalArgumentException("Invalid numeric arguments");
            }
            return new Config(url, durationSeconds, parallelism, pauseMillis, connectTimeoutSeconds, responseTimeoutSeconds);
        }

        private static String requireValue(String option, String[] args, int index) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }

        private static void printUsage() {
            System.out.println("Usage: java scripts/JanusWsLoad.java [options]");
            System.out.println("  --url <wsUrl>                        Default: " + DEFAULT_URL);
            System.out.println("  --duration-seconds <sec>             Default: 70");
            System.out.println("  --parallelism <n>                    Default: 4");
            System.out.println("  --pause-millis <ms>                  Default: 0");
            System.out.println("  --connect-timeout-seconds <sec>      Default: 10");
            System.out.println("  --response-timeout-seconds <sec>     Default: 10");
        }
    }

    private static final class WsSession implements WebSocket.Listener {
        private final CompletableFuture<WebSocket> opened = new CompletableFuture<>();
        private final BlockingQueue<String> texts = new LinkedBlockingQueue<>();
        private final StringBuilder current = new StringBuilder();
        private volatile WebSocket webSocket;

        static WsSession connect(String url, int connectTimeoutSeconds) {
            WsSession listener = new WsSession();
            HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
                    .build()
                    .newWebSocketBuilder()
                    .buildAsync(URI.create(url), listener)
                    .join();
            listener.opened.join();
            return listener;
        }

        void sendText(String payload) {
            webSocket.sendText(payload, true).join();
        }

        String awaitText(int timeoutSeconds) throws Exception {
            String text = texts.poll(timeoutSeconds, TimeUnit.SECONDS);
            if (text == null) {
                throw new IllegalStateException("Timed out waiting for WebSocket response");
            }
            return text;
        }

        void close() {
            if (webSocket != null) {
                try {
                    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done-" + UUID.randomUUID()).join();
                } catch (Exception ignored) {
                }
            }
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            this.webSocket = webSocket;
            webSocket.request(1);
            opened.complete(webSocket);
        }

        @Override
        public CompletableFuture<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            current.append(data);
            if (last) {
                texts.offer(current.toString());
                current.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletableFuture<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            if (!opened.isDone()) {
                opened.completeExceptionally(new IllegalStateException("Closed during handshake: " + reason));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            if (!opened.isDone()) {
                opened.completeExceptionally(error);
            }
        }
    }
}
