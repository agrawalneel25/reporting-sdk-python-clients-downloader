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
PASS rejects server without range support
```

The tests start an in-process HTTP server using `com.sun.net.httpserver.HttpServer`. The server supports `HEAD`, byte ranges, delayed responses, and one forced transient failure. That lets the tests check correctness, parallelism, retry behavior, and validation without relying on a live external server.

## Run against a local Apache server

Start the server as suggested in the task:

```powershell
docker run --rm -p 8080:80 -v ${PWD}:/usr/local/apache2/htdocs/ httpd:latest
```

Compile and run:

```powershell
.\run-tests.ps1
java -cp out dev.neel.downloader.Main http://localhost:8080/my-local-file.txt downloaded.bin 1048576 8
```

Arguments:

- URL
- output path
- chunk size in bytes, optional
- worker count, optional

## Files

- `src/main/java/dev/neel/downloader/ParallelFileDownloader.java` - downloader logic
- `src/main/java/dev/neel/downloader/DownloadConfig.java` - chunk size, worker count, retry count, timeout, progress callback
- `src/main/java/dev/neel/downloader/Main.java` - small CLI wrapper
- `src/test/java/dev/neel/downloader/ParallelFileDownloaderTest.java` - unit-style tests with a local range server

## Limits

This version assumes the server follows the contract in the task: it must return `Accept-Ranges: bytes`, a valid `Content-Length`, and `206 Partial Content` for range requests. I did not add resume-from-existing-file support because the task asks for a downloader that assembles one complete file, and retrying individual chunks covers the failure case I wanted to test.

