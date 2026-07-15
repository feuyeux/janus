[CmdletBinding()]
param(
    [ValidateRange(1, 86400)]
    [int]$CpuDuration = 30,
    [ValidateRange(1, 86400)]
    [int]$MemDuration = 30,
    [string]$MemEvent = 'alloc',
    [ValidateRange(1, 512)]
    [int]$Parallelism = 4,
    [ValidateRange(0, 60000)]
    [int]$PauseMillis = 0,
    [string]$WsUrl = 'ws://127.0.0.1:8080/json',
    [switch]$Live,
    [switch]$ForceRestart
)

$ErrorActionPreference = 'Stop'

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$runDir = Join-Path $projectRoot '.run'
$loadLog = Join-Path $runDir 'janus-load.log'
$loadExtraSeconds = 10

New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Write-Host '[1/3] Starting Janus'
$startArgs = @()
if ($ForceRestart) {
    $startArgs += '-ForceRestart'
}
& (Join-Path $PSScriptRoot 'janus-local-start.ps1') @startArgs
if ($LASTEXITCODE -ne 0) {
    throw 'janus-local-start.ps1 failed'
}

$loadDuration = $CpuDuration + $MemDuration + $loadExtraSeconds
Write-Host "[2/3] Generating Janus load for $loadDuration s"
$requestArgs = @(
    '-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass',
    '-File', (Join-Path $PSScriptRoot 'janus-request.ps1'),
    '-Url', $WsUrl,
    '-DurationSeconds', $loadDuration,
    '-Parallelism', $Parallelism,
    '-PauseMillis', $PauseMillis
)
$loadProcess = Start-Process -FilePath powershell -ArgumentList $requestArgs -RedirectStandardOutput $loadLog -RedirectStandardError $loadLog -PassThru

try {
    Start-Sleep -Seconds 2
    Write-Host '[3/3] Capturing flame graphs'
    $flameArgs = @(
        '-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass',
        '-File', (Join-Path $PSScriptRoot 'arthas-flame.ps1'),
        '-CpuDuration', $CpuDuration,
        '-MemDuration', $MemDuration,
        '-MemEvent', $MemEvent
    )
    if ($Live) {
        $flameArgs += '-Live'
    }
    $flame = Start-Process -FilePath powershell -ArgumentList $flameArgs -Wait -NoNewWindow -PassThru
    if ($flame.ExitCode -ne 0) {
        throw 'arthas-flame.ps1 failed'
    }
    Wait-Process -Id $loadProcess.Id
}
finally {
    if (Get-Process -Id $loadProcess.Id -ErrorAction SilentlyContinue) {
        Stop-Process -Id $loadProcess.Id -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "Load log: $loadLog"
