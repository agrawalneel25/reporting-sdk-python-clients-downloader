package dev.neel.downloader;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        CliArgs cli = CliArgs.parse(args);
        if (cli == null) {
            System.err.println("Usage: java dev.neel.downloader.Main <url> <output-path> "
                    + "[--chunk-bytes N] [--workers N] [--attempts N] [--timeout-seconds N] "
                    + "[--resume true|false] [--sha256 HEX]");
            System.exit(2);
        }

        AtomicLong lastPrinted = new AtomicLong();

        DownloadConfig config = DownloadConfig.builder()
                .chunkSizeBytes(cli.chunkBytes())
                .workers(cli.workers())
                .maxAttempts(cli.attempts())
                .requestTimeout(Duration.ofSeconds(cli.timeoutSeconds()))
                .resumeEnabled(cli.resume())
                .progressListener(done -> {
                    long mib = done / (1024 * 1024);
                    if (mib > lastPrinted.getAndSet(mib)) {
                        System.out.println("downloaded " + mib + " MiB");
                    }
                })
                .build();

        ParallelFileDownloader downloader = new ParallelFileDownloader(config);
        DownloadResult result = downloader.download(URI.create(cli.url()), Path.of(cli.outputPath()));
        System.out.println("downloaded " + result.bytesDownloaded() + " bytes to " + result.destination());
        System.out.println("chunks=" + result.chunkCount() + ", workers=" + result.workerCount());
        if (result.resumed()) {
            System.out.println("resumed from " + result.reusedChunks() + " completed chunks");
        }
        if (!cli.sha256().isBlank()) {
            String actual = sha256(result.destination());
            if (!actual.equalsIgnoreCase(cli.sha256())) {
                throw new IllegalStateException("SHA-256 mismatch: expected " + cli.sha256() + ", got " + actual);
            }
            System.out.println("sha256 verified: " + actual);
        }
    }

    private static String sha256(Path path) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    private record CliArgs(
            String url,
            String outputPath,
            int chunkBytes,
            int workers,
            int attempts,
            int timeoutSeconds,
            boolean resume,
            String sha256
    ) {
        static CliArgs parse(String[] args) {
            if (args.length < 2) {
                return null;
            }

            String url = args[0];
            String outputPath = args[1];
            int chunkBytes = 1024 * 1024;
            int workers = Math.max(2, Runtime.getRuntime().availableProcessors());
            int attempts = 3;
            int timeoutSeconds = 30;
            boolean resume = false;
            String sha256 = "";

            int i = 2;
            while (i < args.length) {
                if (i + 1 >= args.length) {
                    return null;
                }
                String name = args[i];
                String value = args[i + 1];
                try {
                    switch (name) {
                        case "--chunk-bytes" -> chunkBytes = Integer.parseInt(value);
                        case "--workers" -> workers = Integer.parseInt(value);
                        case "--attempts" -> attempts = Integer.parseInt(value);
                        case "--timeout-seconds" -> timeoutSeconds = Integer.parseInt(value);
                        case "--resume" -> resume = Boolean.parseBoolean(value);
                        case "--sha256" -> sha256 = value;
                        default -> {
                            return null;
                        }
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
                i += 2;
            }

            return new CliArgs(url, outputPath, chunkBytes, workers, attempts, timeoutSeconds, resume, sha256);
        }
    }
}
