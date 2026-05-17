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
                new TestCase("rejects missing HEAD content length", ParallelFileDownloaderTest::rejectsMissingHeadContentLength),
                new TestCase("rejects range response with HTTP 200", ParallelFileDownloaderTest::rejectsRangeResponseWithHttp200),
                new TestCase("rejects truncated chunk body", ParallelFileDownloaderTest::rejectsTruncatedChunkBody),
                new TestCase("rejects wrong Content-Range", ParallelFileDownloaderTest::rejectsWrongContentRange),
                new TestCase("removes partial file after failure", ParallelFileDownloaderTest::removesPartialFileAfterFailure),
                new TestCase("resumes from completed chunks", ParallelFileDownloaderTest::resumesFromCompletedChunks),
                new TestCase("does not resume without identity headers", ParallelFileDownloaderTest::doesNotResumeWithoutIdentityHeaders),
                new TestCase("downloads single chunk when content smaller than chunk size", ParallelFileDownloaderTest::downloadsSingleChunkWhenContentSmallerThanChunkSize)
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
            try {
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
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void usesParallelRangeRequests() throws Exception {
        byte[] source = bytes(512 * 1024);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.sleepPerRequestMillis(80);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(32 * 1024)
                        .workers(8)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                new ParallelFileDownloader(config).download(server.uri(), target);
                assertTrue(server.maxConcurrentGets() > 1, "server only saw one GET at a time");
                assertArrayEquals(source, Files.readAllBytes(target), "parallel download was corrupt");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void retriesTransientChunkFailure() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.failFirstGetForStart(0);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(100_000)
                        .workers(3)
                        .maxAttempts(2)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                new ParallelFileDownloader(config).download(server.uri(), target);
                assertArrayEquals(source, Files.readAllBytes(target), "retry download was corrupt");
                assertTrue(server.getCount() > 3, "expected one extra GET after retry");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void rejectsServerWithoutRangeSupport() throws Exception {
        byte[] source = bytes(1024);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.advertiseRanges(false);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(128)
                        .workers(2)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                IOException error = expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(config).download(server.uri(), target)
                );
                assertTrue(error.getMessage().contains("byte range"), "unexpected error: " + error.getMessage());
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void rejectsMissingHeadContentLength() throws Exception {
        byte[] source = bytes(1024);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.includeHeadContentLength(false);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                IOException error = expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(DownloadConfig.defaults()).download(server.uri(), target)
                );
                assertTrue(error.getMessage().contains("Content-Length"), "unexpected error: " + error.getMessage());
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void rejectsRangeResponseWithHttp200() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.returnOkForRanges(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(512)
                        .workers(2)
                        .maxAttempts(1)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                IOException error = expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(config).download(server.uri(), target)
                );
                assertTrue(error.getMessage().contains("HTTP 200"), "unexpected error: " + error.getMessage());
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void rejectsTruncatedChunkBody() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.truncateChunkBody(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(512)
                        .workers(2)
                        .maxAttempts(1)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                IOException error = expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(config).download(server.uri(), target)
                );
                assertTrue(error.getMessage().contains("expected"), "unexpected error: " + error.getMessage());
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void doesNotOvercountProgressAfterRetry() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.failFirstGetForStart(0);
            AtomicLong progress = new AtomicLong();
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(100_000)
                        .workers(3)
                        .maxAttempts(2)
                        .requestTimeout(Duration.ofSeconds(5))
                        .progressListener(progress::set)
                        .build();

                new ParallelFileDownloader(config).download(server.uri(), target);
                assertEquals(source.length, progress.get(), "progress");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void rejectsWrongContentRange() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.shiftContentRangeHeaderByOne(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
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
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void removesPartialFileAfterFailure() throws Exception {
        byte[] source = bytes(2048);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.shiftContentRangeHeaderByOne(true);
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            Path partial = target.resolveSibling(target.getFileName() + ".part");
            try {
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
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(partial);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void resumesFromCompletedChunks() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            Path partial = target.resolveSibling(target.getFileName() + ".part");
            Path manifest = target.resolveSibling(target.getFileName() + ".part.manifest");
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(100_000)
                        .workers(1)
                        .maxAttempts(1)
                        .requestTimeout(Duration.ofSeconds(5))
                        .resumeEnabled(true)
                        .build();

                server.failEveryGetForStart(100_000);
                expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(config).download(server.uri(), target)
                );
                assertTrue(Files.exists(partial), "resume partial file missing");
                assertTrue(Files.exists(manifest), "resume manifest missing");
                int firstRunRequests = server.requestedStarts().size();

                server.stopFailingEveryGet();
                DownloadResult result = new ParallelFileDownloader(config).download(server.uri(), target);

                assertArrayEquals(source, Files.readAllBytes(target), "resumed download was corrupt");
                assertTrue(result.resumed(), "result did not report resume");
                assertTrue(result.reusedChunks() >= 1, "expected at least one reused chunk");
                List<Long> secondRunStarts = server.requestedStarts().subList(firstRunRequests, server.requestedStarts().size());
                assertTrue(!secondRunStarts.contains(0L), "second run re-requested the completed first chunk");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(partial);
                Files.deleteIfExists(manifest);
            }
        }
    }

    private static void doesNotResumeWithoutIdentityHeaders() throws Exception {
        byte[] source = bytes(300_000);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.identityHeaders("", "");
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(100_000)
                        .workers(1)
                        .maxAttempts(1)
                        .requestTimeout(Duration.ofSeconds(5))
                        .resumeEnabled(true)
                        .build();

                server.failEveryGetForStart(100_000);
                expectThrows(IOException.class, () ->
                        new ParallelFileDownloader(config).download(server.uri(), target)
                );
                int firstRunRequests = server.requestedStarts().size();

                server.stopFailingEveryGet();
                DownloadResult result = new ParallelFileDownloader(config).download(server.uri(), target);

                assertArrayEquals(source, Files.readAllBytes(target), "fresh retry was corrupt");
                assertTrue(!result.resumed(), "download resumed without identity headers");
                List<Long> secondRunStarts = server.requestedStarts().subList(firstRunRequests, server.requestedStarts().size());
                assertTrue(secondRunStarts.contains(0L), "fresh retry did not request the first chunk");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
        }
    }

    private static void downloadsSingleChunkWhenContentSmallerThanChunkSize() throws Exception {
        byte[] source = bytes(1024);
        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            Path target = Files.createTempFile("parallel-download", ".bin");
            Files.deleteIfExists(target);
            try {
                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(64 * 1024)
                        .workers(4)
                        .requestTimeout(Duration.ofSeconds(5))
                        .build();

                DownloadResult result = new ParallelFileDownloader(config).download(server.uri(), target);
                byte[] actual = Files.readAllBytes(target);

                assertEquals(1, result.chunkCount(), "expected exactly one chunk");
                assertEquals(1, server.getCount(), "expected exactly one GET request");
                assertArrayEquals(source, actual, "single-chunk downloaded bytes differ");
            } finally {
                Files.deleteIfExists(target);
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part"));
                Files.deleteIfExists(target.resolveSibling(target.getFileName() + ".part.manifest"));
            }
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
            int mismatch = Arrays.mismatch(expected, actual);
            throw new AssertionError(message + ": arrays differ at index " + mismatch +
                    " (expected " + expected[mismatch] + ", got " + actual[mismatch] +
                    "); lengths: " + expected.length + " vs " + actual.length);
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
