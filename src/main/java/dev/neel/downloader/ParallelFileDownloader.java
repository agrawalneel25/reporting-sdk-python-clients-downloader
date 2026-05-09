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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
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
        Path manifestPath = destination.resolveSibling(destination.getFileName() + ".part.manifest");
        ResumeManifest manifest = preparePartial(source, remoteFile, chunks, partial, manifestPath);

        ExecutorService executor = Executors.newFixedThreadPool(config.workers());
        AtomicLong totalWritten = new AtomicLong(manifest.completedBytes());
        boolean completed = false;

        try {
            try (FileChannel file = FileChannel.open(
                    partial,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE
            )) {
                file.truncate(remoteFile.length());

                List<Future<Void>> futures = new ArrayList<>(chunks.size());
                for (Chunk chunk : chunks) {
                    if (manifest.isCompleted(chunk)) {
                        continue;
                    }
                    futures.add(executor.submit(downloadChunk(source, chunk, file, totalWritten, manifest)));
                }
                waitForAll(futures);
            }
            completed = true;
        } finally {
            executor.shutdownNow();
            if (!completed) {
                if (!config.resumeEnabled()) {
                    Files.deleteIfExists(partial);
                    Files.deleteIfExists(manifestPath);
                }
            }
        }

        Files.move(partial, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(manifestPath);
        return new DownloadResult(
                source,
                destination,
                remoteFile.length(),
                chunks.size(),
                config.workers(),
                manifest.reusedChunks(),
                manifest.reusedChunks() > 0
        );
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
        return new RemoteFile(
                contentLength,
                response.headers().firstValue("ETag").orElse(""),
                response.headers().firstValue("Last-Modified").orElse("")
        );
    }

    private ResumeManifest preparePartial(
            URI source,
            RemoteFile remoteFile,
            List<Chunk> chunks,
            Path partial,
            Path manifestPath
    ) throws IOException {
        if (!config.resumeEnabled()) {
            Files.deleteIfExists(partial);
            Files.deleteIfExists(manifestPath);
            return ResumeManifest.fresh(source, remoteFile, chunks, manifestPath, config.chunkSizeBytes());
        }

        ResumeManifest existing = ResumeManifest.loadIfMatching(
                source,
                remoteFile,
                chunks,
                manifestPath,
                config.chunkSizeBytes()
        );
        if (existing != null && Files.exists(partial) && Files.size(partial) == remoteFile.length()) {
            return existing;
        }

        Files.deleteIfExists(partial);
        Files.deleteIfExists(manifestPath);
        return ResumeManifest.fresh(source, remoteFile, chunks, manifestPath, config.chunkSizeBytes());
    }

    private Callable<Void> downloadChunk(
            URI source,
            Chunk chunk,
            FileChannel file,
            AtomicLong totalWritten,
            ResumeManifest manifest
    ) {
        return () -> {
            IOException lastFailure = null;
            for (int attempt = 1; attempt <= config.maxAttempts(); attempt++) {
                try {
                    fetchChunk(source, chunk, file, totalWritten, manifest);
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
            AtomicLong totalWritten,
            ResumeManifest manifest
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
        manifest.markCompleted(chunk);
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

    private record RemoteFile(long length, String etag, String lastModified) {
    }

    private record Chunk(long start, long endInclusive) {
        long length() {
            return endInclusive - start + 1;
        }

        static Chunk parse(String encoded) throws IOException {
            int dash = encoded.indexOf('-');
            if (dash < 0) {
                throw new IOException("Bad completed chunk in manifest: " + encoded);
            }
            try {
                long start = Long.parseLong(encoded.substring(0, dash));
                long end = Long.parseLong(encoded.substring(dash + 1));
                if (start < 0 || end < start) {
                    throw new IOException("Bad completed chunk in manifest: " + encoded);
                }
                return new Chunk(start, end);
            } catch (NumberFormatException e) {
                throw new IOException("Bad completed chunk in manifest: " + encoded, e);
            }
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

    private static final class ResumeManifest {
        private final URI source;
        private final RemoteFile remoteFile;
        private final Path manifestPath;
        private final int chunkSizeBytes;
        private final Set<Chunk> completed;
        private final int reusedChunks;

        private ResumeManifest(
                URI source,
                RemoteFile remoteFile,
                Path manifestPath,
                int chunkSizeBytes,
                Set<Chunk> completed,
                int reusedChunks
        ) {
            this.source = source;
            this.remoteFile = remoteFile;
            this.manifestPath = manifestPath;
            this.chunkSizeBytes = chunkSizeBytes;
            this.completed = completed;
            this.reusedChunks = reusedChunks;
        }

        static ResumeManifest fresh(
                URI source,
                RemoteFile remoteFile,
                List<Chunk> chunks,
                Path manifestPath,
                int chunkSizeBytes
        ) throws IOException {
            ResumeManifest manifest = new ResumeManifest(
                    source,
                    remoteFile,
                    manifestPath,
                    chunkSizeBytes,
                    new HashSet<>(),
                    0
            );
            manifest.persist();
            return manifest;
        }

        static ResumeManifest loadIfMatching(
                URI source,
                RemoteFile remoteFile,
                List<Chunk> chunks,
                Path manifestPath,
                int chunkSizeBytes
        ) throws IOException {
            if (remoteFile.etag().isBlank() && remoteFile.lastModified().isBlank()) {
                return null;
            }
            if (!Files.exists(manifestPath)) {
                return null;
            }

            Properties properties = new Properties();
            try (var input = Files.newInputStream(manifestPath)) {
                properties.load(input);
            }

            if (!source.toString().equals(properties.getProperty("url"))) {
                return null;
            }
            if (!Long.toString(remoteFile.length()).equals(properties.getProperty("length"))) {
                return null;
            }
            if (!Integer.toString(chunkSizeBytes).equals(properties.getProperty("chunkSize"))) {
                return null;
            }
            if (!remoteFile.etag().equals(properties.getProperty("etag", ""))) {
                return null;
            }
            if (!remoteFile.lastModified().equals(properties.getProperty("lastModified", ""))) {
                return null;
            }

            Set<Chunk> completed = new HashSet<>();
            Set<Chunk> validChunks = new HashSet<>(chunks);
            String encoded = properties.getProperty("completed", "");
            if (!encoded.isBlank()) {
                for (String token : encoded.split(",")) {
                    Chunk chunk = Chunk.parse(token);
                    if (validChunks.contains(chunk)) {
                        completed.add(chunk);
                    }
                }
            }
            return new ResumeManifest(source, remoteFile, manifestPath, chunkSizeBytes, completed, completed.size());
        }

        synchronized boolean isCompleted(Chunk chunk) {
            return completed.contains(chunk);
        }

        synchronized void markCompleted(Chunk chunk) throws IOException {
            completed.add(chunk);
            persist();
        }

        synchronized long completedBytes() {
            long total = 0;
            for (Chunk chunk : completed) {
                total += chunk.length();
            }
            return total;
        }

        int reusedChunks() {
            return reusedChunks;
        }

        private void persist() throws IOException {
            Properties properties = new Properties();
            properties.setProperty("url", source.toString());
            properties.setProperty("length", Long.toString(remoteFile.length()));
            properties.setProperty("chunkSize", Integer.toString(chunkSizeBytes));
            properties.setProperty("etag", remoteFile.etag());
            properties.setProperty("lastModified", remoteFile.lastModified());
            properties.setProperty("completed", encodeCompleted());
            try (var output = Files.newOutputStream(
                    manifestPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                properties.store(output, "parallel range downloader resume manifest");
            }
        }

        private String encodeCompleted() {
            List<Chunk> ordered = new ArrayList<>(completed);
            ordered.sort((left, right) -> Long.compare(left.start(), right.start()));
            List<String> encoded = new ArrayList<>(ordered.size());
            for (Chunk chunk : ordered) {
                encoded.add(chunk.start() + "-" + chunk.endInclusive());
            }
            return String.join(",", encoded);
        }
    }
}
