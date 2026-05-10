# Design Notes

The base task is small: split a file into byte ranges, download those ranges in parallel, and assemble the output. I pushed on the parts that matter for a client library rather than adding unrelated surface area.

## Flow

```mermaid
flowchart LR
    Head["HEAD request"] --> Validate["validate Accept-Ranges and Content-Length"]
    Validate --> Split["split into byte ranges"]
    Split --> Get["parallel ranged GET requests"]
    Get --> Check["validate 206, Content-Range, byte count"]
    Check --> Write["write chunks at file offsets"]
    Write --> Move["atomic move from .part to final file"]
```

## Iteration

First pass:

- `HEAD` validation for `Accept-Ranges` and `Content-Length`
- fixed-size chunk planning
- parallel `GET` requests with `Range`
- direct offset writes with `FileChannel`
- local range-server tests

Second pass:

- validate each `206 Partial Content` response with `Content-Range`
- delete `.part` files after failed non-resumable downloads
- count progress only after a chunk fully succeeds

Third pass:

- add a latency benchmark with SHA-256 checks
- pull the range-server test fixture into a shared helper

Final extension:

- optional resume mode with a sidecar manifest
- reuse completed chunks only when URL, length, chunk size, `ETag`, and `Last-Modified` match
- keep the failure mode conservative when identity headers are missing
- test both sides of that rule: resume with identity headers, start clean without them

## Why This Matters For Data Ingestion

For a reporting SDK or a Python client wrapper around a JVM service, the file download is not just transport. It is often the step before a notebook, dashboard, or batch job reads the export and treats it as ground truth.

That changes the priorities:

- A corrupt complete-looking file is worse than a failed download.
- Retrying a whole multi-GB export after one chunk fails wastes time and server bandwidth.
- Resume support is only safe if the client can prove the partial file belongs to the same remote object.

The downloader follows those constraints. It rejects inconsistent range metadata, writes into a temporary `.part` file, and only resumes when the remote identity still matches.

## What I Did Not Add

I did not add checksum verification against a server-published digest because the task server contract does not include one. The CLI does support `--sha256` when the caller already has an expected digest.

I did not add adaptive chunk sizing. The benchmark shows parallelism is already visible under per-request latency, and adaptive sizing would need a larger measurement setup to be more than guesswork.

