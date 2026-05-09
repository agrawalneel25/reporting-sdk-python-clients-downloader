# Parallel Range Downloader

This is my solution for the JetBrains Data Ingestion test task. It downloads a file from an HTTP server by splitting the byte range into chunks, fetching those chunks in parallel, and writing each chunk directly into the right offset in the output file.

The code uses only the JDK. I chose that to keep the submission easy to run: no Gradle, Maven, Docker, or external test dependency is needed for the core tests.

## How it works

`ParallelFileDownloader` starts with a `HEAD` request. It requires two headers from the server:

- `Accept-Ranges: bytes`
- `Content-Length: <number of bytes>`

It then splits the file into fixed-size chunks. Each worker sends a `GET` request with a header like:

```text
Range: bytes=1048576-2097151
```

Every chunk is written through `FileChannel.write(buffer, offset)`, so the downloader does not need to keep all chunks in memory before assembly. Failed chunk requests are retried. The final file is first written as `<name>.part` and moved into place only after all chunks finish.

## Run the tests

From this folder:

```powershell
.\run-tests.ps1
```

Expected output:

```text
PASS downloads exact bytes
PASS uses parallel range requests
PASS retries transient chunk failure
PASS does not overcount progress after retry
PASS rejects server without range support
PASS rejects wrong Content-Range
PASS removes partial file after failure
```

The tests start an in-process HTTP server using `com.sun.net.httpserver.HttpServer`. The server supports `HEAD`, byte ranges, delayed responses, malformed range metadata, and one forced transient failure. That lets the tests check correctness, parallelism, retry behavior, response validation, and failure cleanup without relying on a live external server.

## Run against a local Apache server

Start the server as suggested in the task:

```powershell
docker run --rm -p 8080:80 -v ${PWD}:/usr/local/apache2/htdocs/ httpd:latest
```

Compile and run:

```powershell
.\run-tests.ps1
java -cp out dev.neel.downloader.Main http://localhost:8080/my-local-file.txt downloaded.bin --chunk-bytes 1048576 --workers 8
```

Arguments:

- URL
- output path
- `--chunk-bytes`, optional, default 1048576
- `--workers`, optional, default is at least 2
- `--attempts`, optional, default 3
- `--timeout-seconds`, optional, default 30

## Files

- `src/main/java/dev/neel/downloader/ParallelFileDownloader.java` - downloader logic
- `src/main/java/dev/neel/downloader/DownloadConfig.java` - chunk size, worker count, retry count, timeout, progress callback
- `src/main/java/dev/neel/downloader/Main.java` - small CLI wrapper
- `src/test/java/dev/neel/downloader/ParallelFileDownloaderTest.java` - unit-style tests with a local range server
- `src/test/java/dev/neel/downloader/DownloaderBenchmark.java` - local latency benchmark
- `BENCHMARK.md` - benchmark method and measured output

## Measured result

I added a local benchmark because the task is specifically about parallel chunking. It serves an 8 MiB file from the in-process range server, splits it into 32 chunks, and adds 30 ms of delay per chunk response.

| Workers | Chunks | Time ms | SHA-256 ok |
|---:|---:|---:|:---:|
| 1 | 32 | 1369 | yes |
| 2 | 32 | 560 | yes |
| 4 | 32 | 279 | yes |
| 8 | 32 | 162 | yes |

That is an 8.5x improvement from 1 worker to 8 workers in this controlled setup. The benchmark checks the SHA-256 hash after every run, so the speedup is not hiding a corrupted output file.

## Limits

This version assumes the server follows the contract in the task: it must return `Accept-Ranges: bytes`, a valid `Content-Length`, and `206 Partial Content` for range requests. For each chunk, the downloader also checks `Content-Length` and `Content-Range` before accepting the response. I did not add resume-from-existing-file support because the task asks for a downloader that assembles one complete file, and retrying individual chunks covers the failure case I wanted to test.
