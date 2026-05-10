#!/usr/bin/env bash
set -euo pipefail

rm -rf out
mkdir -p out

find src -name '*.java' -print > out/sources.txt
javac -d out @out/sources.txt
java -cp out dev.neel.downloader.ParallelFileDownloaderTest

