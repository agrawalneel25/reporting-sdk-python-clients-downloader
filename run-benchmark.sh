#!/usr/bin/env bash
set -euo pipefail

if [ ! -d out ]; then
  ./run-tests.sh
fi

java -cp out dev.neel.downloader.DownloaderBenchmark

