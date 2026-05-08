package dev.neel.downloader;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Usage: java dev.neel.downloader.Main <url> <output-path> [chunk-bytes] [workers]");
            System.exit(2);
        }

        int chunkBytes = args.length >= 3 ? Integer.parseInt(args[2]) : 1024 * 1024;
        int workers = args.length >= 4 ? Integer.parseInt(args[3]) : Math.max(2, Runtime.getRuntime().availableProcessors());
        AtomicLong lastPrinted = new AtomicLong();

        DownloadConfig config = DownloadConfig.builder()
                .chunkSizeBytes(chunkBytes)
                .workers(workers)
                .maxAttempts(3)
                .requestTimeout(Duration.ofSeconds(30))
                .progressListener(done -> {
                    long mib = done / (1024 * 1024);
                    if (mib > lastPrinted.getAndSet(mib)) {
                        System.out.println("downloaded " + mib + " MiB");
                    }
                })
                .build();

        ParallelFileDownloader downloader = new ParallelFileDownloader(config);
        DownloadResult result = downloader.download(URI.create(args[0]), Path.of(args[1]));
        System.out.println("downloaded " + result.bytesDownloaded() + " bytes to " + result.destination());
        System.out.println("chunks=" + result.chunkCount() + ", workers=" + result.workerCount());
    }
}

