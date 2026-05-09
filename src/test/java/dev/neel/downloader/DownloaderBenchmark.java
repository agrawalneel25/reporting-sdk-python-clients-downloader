package dev.neel.downloader;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;

public final class DownloaderBenchmark {
    private DownloaderBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        byte[] source = bytes(8 * 1024 * 1024);
        List<Integer> workerCounts = List.of(1, 2, 4, 8);

        try (RangeHttpFixture server = RangeHttpFixture.start(source)) {
            server.sleepPerRequestMillis(30);
            String expectedHash = sha256(source);

            System.out.println("| Workers | Chunks | Time ms | SHA-256 ok |");
            System.out.println("|---:|---:|---:|:---:|");
            for (int workers : workerCounts) {
                Path target = Files.createTempFile("parallel-download-bench", ".bin");
                Files.deleteIfExists(target);

                DownloadConfig config = DownloadConfig.builder()
                        .chunkSizeBytes(256 * 1024)
                        .workers(workers)
                        .requestTimeout(Duration.ofSeconds(10))
                        .build();

                long started = System.nanoTime();
                DownloadResult result = new ParallelFileDownloader(config).download(server.uri(), target);
                long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
                boolean hashOk = expectedHash.equals(sha256(Files.readAllBytes(target)));

                System.out.println("| " + workers + " | " + result.chunkCount() + " | " + elapsedMs + " | "
                        + (hashOk ? "yes" : "no") + " |");
                Files.deleteIfExists(target);
            }
        }
    }

    private static byte[] bytes(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i * 17 + i / 11) & 0xff);
        }
        return data;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}

