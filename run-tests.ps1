$ErrorActionPreference = 'Stop'

if (Test-Path out) {
    Remove-Item -Recurse -Force out
}

New-Item -ItemType Directory -Force -Path out | Out-Null
$files = Get-ChildItem -Recurse -Filter *.java -Path src | ForEach-Object { $_.FullName }

javac -d out @($files)
java -cp out dev.neel.downloader.ParallelFileDownloaderTest

