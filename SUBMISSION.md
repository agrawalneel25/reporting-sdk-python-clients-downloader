# Task #1 - Solution

I built a Java file downloader that uses HTTP byte ranges to download chunks of one file in parallel and assemble them into the target file.

The entry point is:

```text
src/main/java/dev/neel/downloader/ParallelFileDownloader.java
```

The downloader first sends a `HEAD` request and checks that the server advertises byte ranges with `Accept-Ranges: bytes`. It also reads `Content-Length` so it can split the file into fixed byte ranges. Each worker then sends a `GET` request with a `Range` header, for example `Range: bytes=1024-2047`.

I write each chunk straight into its final offset using `FileChannel.write(buffer, position)`. That avoids keeping the full downloaded file in memory and avoids a separate concatenation step. The output is written to a `.part` file first, then moved into place after every chunk finishes.

The main design choice is that I validate the server contract aggressively. A ranged response has to return `206 Partial Content`, a matching `Content-Range`, and the expected byte count. If any chunk fails that check, the partial file is deleted instead of leaving behind output that looks complete but is not trustworthy.

The solution supports:

- configurable chunk size
- configurable worker count
- retrying failed chunk requests
- request timeout
- progress callback
- validation that the server supports byte ranges
- validation that every chunk response matches the requested `Content-Range`
- cleanup of the `.part` file if the download fails
- optional resume from completed chunks when the server exposes stable identity headers

I also added a small CLI:

```text
src/main/java/dev/neel/downloader/Main.java
```

Example:

```powershell
java -cp out dev.neel.downloader.Main http://localhost:8080/my-local-file.txt downloaded.bin --chunk-size 1MB --workers 8 --resume true
```

The tests are in:

```text
src/test/java/dev/neel/downloader/ParallelFileDownloaderTest.java
```

They start a local HTTP server in-process and verify:

- byte-for-byte correctness on a multi-chunk file
- actual parallel range requests
- retry after one transient chunk failure
- progress accounting after retry
- rejection of a server that does not advertise byte range support
- rejection of a missing `Content-Length` on `HEAD`
- rejection of a ranged response that returns `200 OK`
- rejection of a truncated chunk body
- rejection of a wrong `Content-Range`
- cleanup of partial output after failure
- safe resume from already completed chunks

I kept the project dependency-free so it can run with only a JDK. On my machine, this command passes:

```powershell
.\run-tests.ps1
```

Output:

```text
PASS downloads exact bytes
PASS uses parallel range requests
PASS retries transient chunk failure
PASS does not overcount progress after retry
PASS rejects server without range support
PASS rejects missing HEAD content length
PASS rejects range response with HTTP 200
PASS rejects truncated chunk body
PASS rejects wrong Content-Range
PASS removes partial file after failure
PASS resumes from completed chunks
PASS does not resume without identity headers
```

I also added a local benchmark:

```text
src/test/java/dev/neel/downloader/DownloaderBenchmark.java
```

It serves an 8 MiB file from the same in-process range server, splits it into 32 chunks, and adds 30 ms of delay per chunk response. One local run on my machine:

| Workers | Chunks | Time ms | SHA-256 ok |
|---:|---:|---:|:---:|
| 1 | 32 | 1370 | yes |
| 2 | 32 | 590 | yes |
| 4 | 32 | 294 | yes |
| 8 | 32 | 161 | yes |

That is about 8.5x faster with 8 workers than with 1 worker in this controlled setup. The benchmark verifies the SHA-256 hash after each run, so it checks both speed and correctness.

The tie-in I had in mind for the Data Ingestion projects is that SDK downloads should fail loudly and be resumable when the server gives enough metadata. A reporting SDK client that silently accepts a corrupt export is worse than one that errors; downstream analysis can look valid while being wrong. That is why I spent most of the extra work on range validation, `.part` cleanup, and conservative resume semantics rather than adding unrelated features.
