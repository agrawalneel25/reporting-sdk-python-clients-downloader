package dev.neel.downloader;

import java.net.URI;
import java.nio.file.Path;

public record DownloadResult(
        URI source,
        Path destination,
        long bytesDownloaded,
        int chunkCount,
        int workerCount,
        int reusedChunks,
        boolean resumed
) {
}
