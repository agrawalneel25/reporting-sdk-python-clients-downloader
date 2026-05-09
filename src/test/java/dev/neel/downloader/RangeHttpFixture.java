package dev.neel.downloader;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class RangeHttpFixture implements AutoCloseable {
    private final byte[] content;
    private final HttpServer server;
    private final ExecutorService executor;
    private final AtomicInteger activeGets = new AtomicInteger();
    private final AtomicInteger maxConcurrentGets = new AtomicInteger();
    private final AtomicInteger getCount = new AtomicInteger();
    private final AtomicLong failFirstStart = new AtomicLong(-1);
    private final AtomicLong failAlwaysStart = new AtomicLong(-1);
    private final List<Long> requestedStarts = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean failFirstArmed;
    private volatile boolean advertiseRanges = true;
    private volatile boolean shiftContentRangeHeaderByOne;
    private volatile String etag = "\"fixture-v1\"";
    private volatile String lastModified = "Mon, 11 May 2026 12:00:00 GMT";
    private volatile int sleepPerRequestMillis;

    private RangeHttpFixture(byte[] content, HttpServer server, ExecutorService executor) {
        this.content = content;
        this.server = server;
        this.executor = executor;
    }

    static RangeHttpFixture start(byte[] content) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        RangeHttpFixture fixture = new RangeHttpFixture(content, http, executor);
        http.createContext("/file.bin", fixture::handle);
        http.setExecutor(executor);
        http.start();
        return fixture;
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

    void failEveryGetForStart(long start) {
        failAlwaysStart.set(start);
    }

    void stopFailingEveryGet() {
        failAlwaysStart.set(-1);
    }

    void shiftContentRangeHeaderByOne(boolean shiftContentRangeHeaderByOne) {
        this.shiftContentRangeHeaderByOne = shiftContentRangeHeaderByOne;
    }

    void identityHeaders(String etag, String lastModified) {
        this.etag = etag;
        this.lastModified = lastModified;
    }

    int maxConcurrentGets() {
        return maxConcurrentGets.get();
    }

    int getCount() {
        return getCount.get();
    }

    List<Long> requestedStarts() {
        synchronized (requestedStarts) {
            return List.copyOf(requestedStarts);
        }
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
        if (!etag.isBlank()) {
            headers.add("ETag", etag);
        }
        if (!lastModified.isBlank()) {
            headers.add("Last-Modified", lastModified);
        }
        exchange.sendResponseHeaders(200, -1);
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        int active = activeGets.incrementAndGet();
        maxConcurrentGets.accumulateAndGet(active, Math::max);
        getCount.incrementAndGet();
        try {
            Range range = parseRange(exchange.getRequestHeaders().getFirst("Range"));
            requestedStarts.add(range.start());
            if (failAlwaysStart.get() == range.start()) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            if (shouldFailOnce(range.start())) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            if (sleepPerRequestMillis > 0) {
                sleep(sleepPerRequestMillis);
            }

            int length = (int) (range.endInclusive() - range.start() + 1);
            long headerStart = shiftContentRangeHeaderByOne ? range.start() + 1 : range.start();
            Headers headers = exchange.getResponseHeaders();
            headers.add("Accept-Ranges", "bytes");
            headers.add("Content-Range", "bytes " + headerStart + "-" + range.endInclusive() + "/" + content.length);
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

    private record Range(long start, long endInclusive) {
    }
}
