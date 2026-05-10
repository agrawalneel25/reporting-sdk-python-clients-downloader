# Local Benchmark

This benchmark is not meant to claim internet download speed. It isolates the part this task is about: splitting one file into byte ranges and fetching those ranges concurrently.

The benchmark starts the same in-process range server used by the tests, serves an 8 MiB file, splits it into 32 chunks of 256 KiB, and adds 30 ms of delay to each chunk response. That simulates a server where per-request latency matters. Each run verifies the SHA-256 hash of the assembled file.

Command:

```powershell
.\run-tests.ps1
java -cp out dev.neel.downloader.DownloaderBenchmark
```

One local run on my machine:

| Workers | Chunks | Time ms | SHA-256 ok |
|---:|---:|---:|:---:|
| 1 | 32 | 1370 | yes |
| 2 | 32 | 590 | yes |
| 4 | 32 | 294 | yes |
| 8 | 32 | 161 | yes |

The 8-worker run is about 8.5x faster than the 1-worker run in this setup. The important point is not the exact number, it is that the downloader actually overlaps range requests and still produces identical bytes.


