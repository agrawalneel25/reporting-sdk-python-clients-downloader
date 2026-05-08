package dev.neel.downloader;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ParallelFileDownloaderTest {
    private ParallelFileDownloaderTest() {
    }

    public static void main(String[] args) throws Exception {
        List<TestCase> tests = List.of(
                new TestCase("downloads exact bytes", ParallelFileDownloaderTest::downloadsExactBytes),
                new TestCase("uses parallel range requests", ParallelFileDownloaderTest::usesParallelRangeRequests),
                new TestCase("retries transient chunk failure", ParallelFileDownloaderTest::retriesTransientChunkFailure),
                new TestCase("rejects server without range support", ParallelFileDownloaderTest::rejectsServerWithoutRangeSupport)
        );

        for (TestCase test : tests) {
            test.body().run();
            System.out.println("PASS " + test.name());
        }
    }

    private static void downloadsExactBytes() throws Exception {
        byte[] source = bytes(2_500_003);
        try (RangeTestServer server = RangeTestServer.start(source)) {
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(64 * 1024)
                    .workers(6)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            DownloadResult result = new ParallelFileDownloader(config).download(server.uri(), target);
            byte[] actual = Files.readAllBytes(target);

            assertEquals(source.length, result.bytesDownloaded(), "bytesDownloaded");
            assertTrue(result.chunkCount() > 1, "expected multiple chunks");
            assertArrayEquals(source, actual, "downloaded bytes changed");
            assertEquals(sha256(source), sha256(actual), "sha256");
        }
    }

    private static void usesParallelRangeRequests() throws Exception {
        byte[] source = bytes(512 * 1024);
        try (RangeTestServer server = RangeTestServer.start(source)) {
            server.sleepPerRequestMillis(80);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(32 * 1024)
                    .workers(8)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            new ParallelFileDownloader(config).download(server.uri(), target);
            assertTrue(server.maxConcurrentGets() > 1, "server only saw one GET at a time");
            assertArrayEquals(source, Files.readAllBytes(target), "parallel download was corrupt");
        }
    }

    private static void retriesTransientChunkFailure() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeTestServer server = RangeTestServer.start(source)) {
            server.failFirstGetForStart(0);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(100_000)
                    .workers(3)
                    .maxAttempts(2)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            new ParallelFileDownloader(config).download(server.uri(), target);
            assertArrayEquals(source, Files.readAllBytes(target), "retry download was corrupt");
            assertTrue(server.getCount() > 3, "expected one extra GET after retry");
        }
    }

    private static void rejectsServerWithoutRangeSupport() throws Exception {
        byte[] source = bytes(1024);
        try (RangeTestServer server = RangeTestServer.start(source)) {
            server.advertiseRanges(false);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(128)
                    .workers(2)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            IOException error = expectThrows(IOException.class, () ->
                    new ParallelFileDownloader(config).download(server.uri(), target)
            );
            assertTrue(error.getMessage().contains("byte range"), "unexpected error: " + error.getMessage());
        }
    }

    private static byte[] bytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i * 31 + i / 7) & 0xff);
        }
        return data;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + ": expected " + expected.length + " bytes, got " + actual.length);
        }
    }

    private static void assertEquals(long expected, long actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + ": expected " + expected + ", got " + actual);
        }
    }

    private static void assertEquals(String expected, String actual, String message) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message + ": expected " + expected + ", got " + actual);
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static <T extends Throwable> T expectThrows(Class<T> type, ThrowingRunnable body) throws Exception {
        try {
            body.run();
        } catch (Throwable error) {
            if (type.isInstance(error)) {
                return type.cast(error);
            }
            throw new AssertionError("expected " + type.getSimpleName() + ", got " + error.getClass().getSimpleName(), error);
        }
        throw new AssertionError("expected " + type.getSimpleName());
    }

    private record TestCase(String name, ThrowingRunnable body) {
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class RangeTestServer implements AutoCloseable {
        private final byte[] content;
        private final HttpServer server;
        private final ExecutorService executor;
        private final AtomicInteger activeGets = new AtomicInteger();
        private final AtomicInteger maxConcurrentGets = new AtomicInteger();
        private final AtomicInteger getCount = new AtomicInteger();
        private final AtomicLong failFirstStart = new AtomicLong(-1);
        private volatile boolean failFirstArmed;
        private volatile boolean advertiseRanges = true;
        private volatile int sleepPerRequestMillis;

        private RangeTestServer(byte[] content, HttpServer server, ExecutorService executor) {
            this.content = content;
            this.server = server;
            this.executor = executor;
        }

        static RangeTestServer start(byte[] content) throws IOException {
            HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            ExecutorService executor = Executors.newCachedThreadPool();
            RangeTestServer rangeServer = new RangeTestServer(content, http, executor);
            http.createContext("/file.bin", rangeServer::handle);
            http.setExecutor(executor);
            http.start();
            return rangeServer;
        }

        URI uri() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/file.bin");
        }

        void advertiseRanges(boolean advertiseRanges) {
            this.advertiseRanges = advertiseRanges;
        }

        void sleepPerRequestMillis(int sleepPerRequestMillis) {
            this.sleepPerRequestMillis = sleepPerRequestMillis;
        }

        void failFirstGetForStart(long start) {
            failFirstStart.set(start);
            failFirstArmed = true;
        }

        int maxConcurrentGets() {
            return maxConcurrentGets.get();
        }

        int getCount() {
            return getCount.get();
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    handleHead(exchange);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    handleGet(exchange);
                } else {
                    exchange.sendResponseHeaders(405, -1);
                }
            } finally {
                exchange.close();
            }
        }

        private void handleHead(HttpExchange exchange) throws IOException {
            Headers headers = exchange.getResponseHeaders();
            if (advertiseRanges) {
                headers.add("Accept-Ranges", "bytes");
            }
            headers.add("Content-Length", Integer.toString(content.length));
            exchange.sendResponseHeaders(200, -1);
        }

        private void handleGet(HttpExchange exchange) throws IOException {
            int active = activeGets.incrementAndGet();
            maxConcurrentGets.accumulateAndGet(active, Math::max);
            getCount.incrementAndGet();
            try {
                Range range = parseRange(exchange.getRequestHeaders().getFirst("Range"));
                if (shouldFailOnce(range.start())) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                if (sleepPerRequestMillis > 0) {
                    sleep(sleepPerRequestMillis);
                }

                int length = (int) (range.endInclusive() - range.start() + 1);
                Headers headers = exchange.getResponseHeaders();
                headers.add("Accept-Ranges", "bytes");
                headers.add("Content-Range", "bytes " + range.start() + "-" + range.endInclusive() + "/" + content.length);
                headers.add("Content-Length", Integer.toString(length));
                exchange.sendResponseHeaders(206, length);
                try (OutputStream body = exchange.getResponseBody()) {
                    body.write(content, (int) range.start(), length);
                }
            } finally {
                activeGets.decrementAndGet();
            }
        }

        private boolean shouldFailOnce(long start) {
            if (failFirstArmed && failFirstStart.get() == start) {
                failFirstArmed = false;
                return true;
            }
            return false;
        }

        private Range parseRange(String header) {
            if (header == null || !header.startsWith("bytes=")) {
                throw new IllegalArgumentException("missing range");
            }
            String[] parts = header.substring("bytes=".length()).split("-", 2);
            long start = Long.parseLong(parts[0]);
            long end = Long.parseLong(parts[1]);
            if (start < 0 || end < start || end >= content.length) {
                throw new IllegalArgumentException("bad range: " + header);
            }
            return new Range(start, end);
        }

        private void sleep(int millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private record Range(long start, long endInclusive) {
    }
}
