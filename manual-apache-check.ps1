$ErrorActionPreference = 'Stop'

$containerName = 'parallel-range-downloader-apache-check'
$sample = Join-Path $PWD 'apache-sample.bin'
$downloaded = Join-Path $PWD 'apache-downloaded.bin'

if (Test-Path $sample) {
    Remove-Item -Force $sample
}
if (Test-Path $downloaded) {
    Remove-Item -Force $downloaded
}

$bytes = New-Object byte[] (2 * 1024 * 1024 + 123)
for ($i = 0; $i -lt $bytes.Length; $i++) {
    $bytes[$i] = [byte](($i * 37 + [math]::Floor($i / 13)) -band 0xff)
}
[System.IO.File]::WriteAllBytes($sample, $bytes)
$expected = (Get-FileHash -Algorithm SHA256 $sample).Hash.ToLowerInvariant()

docker rm -f $containerName 2>$null | Out-Null
docker run -d --rm --name $containerName -p 8080:80 -v "${PWD}:/usr/local/apache2/htdocs/" httpd:latest | Out-Null

try {
    Start-Sleep -Seconds 2
    .\run-tests.ps1
    java -cp out dev.neel.downloader.Main `
        http://localhost:8080/apache-sample.bin `
        apache-downloaded.bin `
        --chunk-bytes 65536 `
        --workers 6 `
        --sha256 $expected

    $actual = (Get-FileHash -Algorithm SHA256 $downloaded).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
        throw "SHA-256 mismatch: expected $expected, got $actual"
    }
    Write-Output "Apache integration check passed: $actual"
} finally {
    docker rm -f $containerName 2>$null | Out-Null
    Remove-Item -Force $sample, $downloaded -ErrorAction SilentlyContinue
}

