# Task #1 - Solution

I built a Java file downloader that uses HTTP byte ranges to download chunks of one file in parallel and assemble them into the target file.

The entry point is:

```text
src/main/java/dev/neel/downloader/ParallelFileDownloader.java
```

The downloader first sends a `HEAD` request and checks that the server advertises byte ranges with `Accept-Ranges: bytes`. It also reads `Content-Length` so it can split the file into fixed byte ranges. Each worker then sends a `GET` request with a `Range` header, for example `Range: bytes=1024-2047`.

I write each chunk straight into its final offset using `FileChannel.write(buffer, position)`. That avoids keeping the full downloaded file in memory and avoids a separate concatenation step. The output is written to a `.part` file first, then moved into place after every chunk finishes.

The solution supports:

- configurable chunk size
- configurable worker count
- retrying failed chunk requests
- request timeout
- progress callback
- validation that the server supports byte ranges

I also added a small CLI:

```text
src/main/java/dev/neel/downloader/Main.java
```

Example:

```powershell
java -cp out dev.neel.downloader.Main http://localhost:8080/my-local-file.txt downloaded.bin 1048576 8
```

The tests are in:

```text
src/test/java/dev/neel/downloader/ParallelFileDownloaderTest.java
```

They start a local HTTP server in-process and verify:

- byte-for-byte correctness on a multi-chunk file
- actual parallel range requests
- retry after one transient chunk failure
- rejection of a server that does not advertise byte range support

I kept the project dependency-free so it can run with only a JDK. On my machine, this command passes:

```powershell
.\run-tests.ps1
```

Output:

```text
PASS downloads exact bytes
PASS uses parallel range requests
PASS retries transient chunk failure
PASS rejects server without range support
```

