package dev.neel.downloader;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        CliArgs cli = CliArgs.parse(args);
        if (cli == null) {
            System.err.println("Usage: java dev.neel.downloader.Main <url> <output-path> "
                    + "[--chunk-bytes N] [--workers N] [--attempts N] [--timeout-seconds N]");
            System.exit(2);
        }

        AtomicLong lastPrinted = new AtomicLong();

        DownloadConfig config = DownloadConfig.builder()
                .chunkSizeBytes(cli.chunkBytes())
                .workers(cli.workers())
                .maxAttempts(cli.attempts())
                .requestTimeout(Duration.ofSeconds(cli.timeoutSeconds()))
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
    }

    private record CliArgs(
            String url,
            String outputPath,
            int chunkBytes,
            int workers,
            int attempts,
            int timeoutSeconds
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
                        default -> {
                            return null;
                        }
                    }
                } catch (NumberFormatException e) {
                    return null;
                }
                i += 2;
            }

            return new CliArgs(url, outputPath, chunkBytes, workers, attempts, timeoutSeconds);
        }
    }
}
