$ErrorActionPreference = 'Stop'

if (-not (Test-Path out)) {
    .\run-tests.ps1
}

java -cp out dev.neel.downloader.DownloaderBenchmark

