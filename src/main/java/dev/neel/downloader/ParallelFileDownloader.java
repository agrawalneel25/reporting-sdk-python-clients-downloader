package dev.neel.downloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public final class ParallelFileDownloader {
    private final HttpClient httpClient;
    private final DownloadConfig config;

    public ParallelFileDownloader(DownloadConfig config) {
        this.config = Objects.requireNonNull(config);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(config.requestTimeout())
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public DownloadResult download(URI source, Path destination) throws IOException, InterruptedException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(destination);

        RemoteFile remoteFile = inspect(source);
        List<Chunk> chunks = split(remoteFile.length(), config.chunkSizeBytes());
        Files.createDirectories(destination.toAbsolutePath().getParent());
        Path partial = destination.resolveSibling(destination.getFileName() + ".part");
        Files.deleteIfExists(partial);

        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        AtomicLong totalWritten = new AtomicLong();
        boolean completed = false;

        try {
            try (FileChannel file = FileChannel.open(
                    partial,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
            )) {
                file.truncate(remoteFile.length());

                List<Future<Void>> futures = new ArrayList<>(chunks.size());
                for (Chunk chunk : chunks) {
                    futures.add(executor.submit(downloadChunk(source, chunk, file, totalWritten)));
                }
                waitForAll(futures);
            }
            completed = true;
        } finally {
            executor.shutdownNow();
            if (!completed) {
                Files.deleteIfExists(partial);
            }
        }

        Files.move(partial, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return new DownloadResult(source, destination, remoteFile.length(), chunks.size(), config.workers());
    }

    private RemoteFile inspect(URI source) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(source)
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(config.requestTimeout())
                .build();

        HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HEAD failed with HTTP " + response.statusCode());
        }

        String acceptRanges = response.headers()
                .firstValue("Accept-Ranges")
                .orElse("")
                .toLowerCase(Locale.ROOT);
        if (!acceptRanges.contains("bytes")) {
            throw new IOException("Server does not advertise byte range support");
        }

        long contentLength = response.headers()
                .firstValueAsLong("Content-Length")
                .orElseThrow(() -> new IOException("HEAD response did not include Content-Length"));
        if (contentLength < 0) {
            throw new IOException("Content-Length must not be negative");
        }
        return new RemoteFile(contentLength);
    }

    private Callable<Void> downloadChunk(
            URI source,
            Chunk chunk,
            FileChannel file,
            AtomicLong totalWritten
    ) {
        return () -> {
            IOException lastFailure = null;
            for (int attempt = 1; attempt <= config.maxAttempts(); attempt++) {
                try {
                    fetchChunk(source, chunk, file, totalWritten);
                    return null;
                } catch (IOException e) {
                    lastFailure = e;
                }
            }
            throw lastFailure;
        };
    }

    private void fetchChunk(
            URI source,
            Chunk chunk,
            FileChannel file,
            AtomicLong totalWritten
    ) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(source)
                .GET()
                .header("Range", "bytes=" + chunk.start() + "-" + chunk.endInclusive())
                .timeout(config.requestTimeout())
                .build();

        HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 206) {
            throw new IOException("Range GET for " + chunk + " returned HTTP " + response.statusCode());
        }
        validateRangeResponse(response, chunk);

        long expected = chunk.length();
        long writtenForChunk = 0;
        byte[] buffer = new byte[Math.min(64 * 1024, (int) Math.max(1, expected))];

        try (InputStream body = response.body()) {
            while (true) {
                int read = body.read(buffer);
                if (read == -1) {
                    break;
                }
                writeFully(file, ByteBuffer.wrap(buffer, 0, read), chunk.start() + writtenForChunk);
                writtenForChunk += read;
            }
        }

        if (writtenForChunk != expected) {
            throw new IOException("Range GET for " + chunk + " returned " + writtenForChunk + " bytes, expected " + expected);
        }
        long current = totalWritten.addAndGet(writtenForChunk);
        config.progressListener().accept(current);
    }

    private static void validateRangeResponse(HttpResponse<?> response, Chunk chunk) throws IOException {
        OptionalLong contentLength = response.headers().firstValueAsLong("Content-Length");
        if (contentLength.isPresent() && contentLength.getAsLong() != chunk.length()) {
            throw new IOException("Content-Length for " + chunk + " was " + contentLength.getAsLong()
                    + ", expected " + chunk.length());
        }

        Optional<String> contentRange = response.headers().firstValue("Content-Range");
        if (contentRange.isEmpty()) {
            throw new IOException("Range GET for " + chunk + " did not include Content-Range");
        }

        ContentRange parsed = ContentRange.parse(contentRange.get());
        if (parsed.start() != chunk.start() || parsed.endInclusive() != chunk.endInclusive()) {
            throw new IOException("Content-Range for " + chunk + " was " + contentRange.get());
        }
    }

    private static void writeFully(FileChannel file, ByteBuffer buffer, long position) throws IOException {
        long offset = position;
        while (buffer.hasRemaining()) {
            offset += file.write(buffer, offset);
        }
    }

    private static void waitForAll(List<Future<Void>> futures) throws IOException, InterruptedException {
        IOException failure = null;
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    failure = io;
                } else if (cause instanceof InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                } else {
                    failure = new IOException(cause);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static List<Chunk> split(long length, int chunkSizeBytes) {
        List<Chunk> chunks = new ArrayList<>();
        for (long start = 0; start < length; start += chunkSizeBytes) {
            long end = Math.min(length - 1, start + chunkSizeBytes - 1L);
            chunks.add(new Chunk(start, end));
        }
        return chunks;
    }

    private record RemoteFile(long length) {
    }

    private record Chunk(long start, long endInclusive) {
        long length() {
            return endInclusive - start + 1;
        }
    }

    private record ContentRange(long start, long endInclusive) {
        static ContentRange parse(String header) throws IOException {
            String prefix = "bytes ";
            if (!header.startsWith(prefix)) {
                throw new IOException("Unsupported Content-Range: " + header);
            }

            int dash = header.indexOf('-', prefix.length());
            int slash = header.indexOf('/', dash + 1);
            if (dash < 0 || slash < 0) {
                throw new IOException("Bad Content-Range: " + header);
            }

            try {
                long start = Long.parseLong(header.substring(prefix.length(), dash));
                long end = Long.parseLong(header.substring(dash + 1, slash));
                if (start < 0 || end < start) {
                    throw new IOException("Bad Content-Range: " + header);
                }
                return new ContentRange(start, end);
            } catch (NumberFormatException e) {
                throw new IOException("Bad Content-Range: " + header, e);
            }
        }
    }
}
