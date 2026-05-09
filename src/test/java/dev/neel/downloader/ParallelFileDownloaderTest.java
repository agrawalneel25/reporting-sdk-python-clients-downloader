package dev.neel.downloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class ParallelFileDownloaderTest {
    private ParallelFileDownloaderTest() {
    }

    public static void main(String[] args) throws Exception {
        List<TestCase> tests = List.of(
                new TestCase("downloads exact bytes", ParallelFileDownloaderTest::downloadsExactBytes),
                new TestCase("uses parallel range requests", ParallelFileDownloaderTest::usesParallelRangeRequests),
                new TestCase("retries transient chunk failure", ParallelFileDownloaderTest::retriesTransientChunkFailure),
                new TestCase("does not overcount progress after retry", ParallelFileDownloaderTest::doesNotOvercountProgressAfterRetry),
                new TestCase("rejects server without range support", ParallelFileDownloaderTest::rejectsServerWithoutRangeSupport),
                new TestCase("rejects wrong Content-Range", ParallelFileDownloaderTest::rejectsWrongContentRange),
                new TestCase("removes partial file after failure", ParallelFileDownloaderTest::removesPartialFileAfterFailure)
        );

        for (TestCase test : tests) {
            test.body().run();
            System.out.println("PASS " + test.name());
        }
    }

    private static void downloadsExactBytes() throws Exception {
        byte[] source = bytes(2_500_003);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
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
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
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
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
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
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
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

    private static void doesNotOvercountProgressAfterRetry() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.failFirstGetForStart(0);
            AtomicLong progress = new AtomicLong();
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(100_000)
                    .workers(3)
                    .maxAttempts(2)
                    .requestTimeout(Duration.ofSeconds(5))
                    .progressListener(progress::set)
                    .build();

            new ParallelFileDownloader(config).download(server.uri(), target);
            assertEquals(source.length, progress.get(), "progress");
        }
    }

    private static void rejectsWrongContentRange() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.shiftContentRangeHeaderByOne(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(512)
                    .workers(2)
                    .maxAttempts(1)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            IOException error = expectThrows(IOException.class, () ->
                    new ParallelFileDownloader(config).download(server.uri(), target)
            );
            assertTrue(error.getMessage().contains("Content-Range"), "unexpected error: " + error.getMessage());
        }
    }

    private static void removesPartialFileAfterFailure() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.shiftContentRangeHeaderByOne(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            Path partial = target.resolveSibling(target.getFileName() + ".part");

            DownloadConfig config = DownloadConfig.builder()
                    .chunkSizeBytes(512)
                    .workers(2)
                    .maxAttempts(1)
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();

            expectThrows(IOException.class, () ->
                    new ParallelFileDownloader(config).download(server.uri(), target)
            );
            assertTrue(!Files.exists(partial), "partial file was left behind: " + partial);
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

}
