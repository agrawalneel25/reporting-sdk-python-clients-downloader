package dev.neel.downloader;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongConsumer;

public final class DownloadConfig {
    private final int chunkSizeBytes;
    private final int workers;
    private final int maxAttempts;
    private final Duration requestTimeout;
    private final LongConsumer progressListener;
    private final boolean resumeEnabled;

    private DownloadConfig(Builder builder) {
        if (builder.chunkSizeBytes <= 0) {
            throw new IllegalArgumentException("chunkSizeBytes must be positive");
        }
        if (builder.workers <= 0) {
            throw new IllegalArgumentException("workers must be positive");
        }
        if (builder.maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.chunkSizeBytes = builder.chunkSizeBytes;
        this.workers = builder.workers;
        this.maxAttempts = builder.maxAttempts;
        this.requestTimeout = Objects.requireNonNull(builder.requestTimeout);
        this.progressListener = Objects.requireNonNull(builder.progressListener);
        this.resumeEnabled = builder.resumeEnabled;
    }

    public int chunkSizeBytes() {
        return chunkSizeBytes;
    }

    public int workers() {
        return workers;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public LongConsumer progressListener() {
        return progressListener;
    }

    public boolean resumeEnabled() {
        return resumeEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DownloadConfig defaults() {
        return builder().build();
    }

    public static final class Builder {
        private int chunkSizeBytes = 1024 * 1024;
        private int workers = Math.max(2, Runtime.getRuntime().availableProcessors());
        private int maxAttempts = 3;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private LongConsumer progressListener = ignored -> { };
        private boolean resumeEnabled;

        public Builder chunkSizeBytes(int chunkSizeBytes) {
            this.chunkSizeBytes = chunkSizeBytes;
            return this;
        }

        public Builder workers(int workers) {
            this.workers = workers;
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder progressListener(LongConsumer progressListener) {
            this.progressListener = progressListener;
            return this;
        }

        public Builder resumeEnabled(boolean resumeEnabled) {
            this.resumeEnabled = resumeEnabled;
            return this;
        }

        public DownloadConfig build() {
            return new DownloadConfig(this);
        }
    }
}
