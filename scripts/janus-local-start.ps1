[CmdletBinding()]
param(
    [int]$WsPort = 8080,
    [int]$GrpcPort = 9090,
    [int]$MetricsPort = 9100,
    [switch]$NoBuild,
    [switch]$ForceRestart,
    [ValidateRange(1, 300)]
    [int]$WaitSeconds = 30
)

$ErrorActionPreference = 'Stop'

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$runDir = Join-Path $projectRoot '.run'
$pidFile = Join-Path $runDir 'janus-local.pid'
$logFile = Join-Path $runDir 'janus-local.log'
$jarFile = Join-Path $projectRoot 'target\janus.jar'

function Fail([string]$Message) {
    throw $Message
}

function Get-JavaCommand {
    if ($env:JAVA_HOME) {
        $javaFromHome = Join-Path $env:JAVA_HOME 'bin\java.exe'
        if (Test-Path $javaFromHome) {
            return $javaFromHome
        }
    }

    $java = Get-Command java -ErrorAction SilentlyContinue
    if ($java) {
        return $java.Source
    }

    Fail 'java not found. Please install JDK/JRE or set JAVA_HOME.'
}

function Test-PidAlive([int]$PidValue) {
    if ($PidValue -le 0) {
        return $false
    }
    return [bool](Get-Process -Id $PidValue -ErrorAction SilentlyContinue)
}

New-Item -ItemType Directory -Path $runDir -Force | Out-Null

if (Test-Path $pidFile) {
    $oldPidText = (Get-Content $pidFile -Raw).Trim()
    $oldPid = 0
    if ([int]::TryParse($oldPidText, [ref]$oldPid) -and (Test-PidAlive $oldPid)) {
        if ($ForceRestart) {
            Write-Host "Stopping existing Janus process: $oldPid"
            Stop-Process -Id $oldPid -Force
            Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        }
        else {
            Write-Host "Janus already running with pid $oldPid"
            Write-Host "Log: $logFile"
            exit 0
        }
    }
    else {
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
}

if (-not (Test-Path $jarFile)) {
    if ($NoBuild) {
        Fail "$jarFile not found. Run mvn package first or omit -NoBuild."
    }
    Write-Host 'Building target/janus.jar with Maven'
    Push-Location $projectRoot
    try {
        & mvn -DskipTests package
        if ($LASTEXITCODE -ne 0) {
            Fail 'Maven build failed'
        }
    }
    finally {
        Pop-Location
    }
}

$javaCmd = Get-JavaCommand
Write-Host "Starting Janus locally on ws=$WsPort grpc=$GrpcPort metrics=$MetricsPort"

$envMap = @{
    JANUS_SERVER_ID = 'local-profiler'
    JANUS_ADVERTISED_HOST = 'localhost'
    JANUS_HOST = '0.0.0.0'
    JANUS_WS_PORT = "$WsPort"
    JANUS_GRPC_PORT = "$GrpcPort"
    JANUS_METRICS_PORT = "$MetricsPort"
    JANUS_DOWNSTREAM_PROTOCOL = 'none'
    JANUS_DOWNSTREAM_DISCOVERY = 'none'
    JANUS_REGISTER = 'none'
    JANUS_OTEL_ENABLED = 'N'
    JANUS_METRICS_ENABLED = 'Y'
}

$commandParts = foreach ($entry in $envMap.GetEnumerator()) {
    '$env:{0}=''{1}''' -f $entry.Key, $entry.Value
}
$commandParts += '& "{0}" -jar "{1}"' -f $javaCmd, $jarFile
$command = ($commandParts -join '; ')

$process = Start-Process -FilePath powershell -ArgumentList @(
    '-NoLogo', '-NoProfile', '-Command', $command
) -WorkingDirectory $projectRoot -RedirectStandardOutput $logFile -RedirectStandardError $logFile -PassThru

Set-Content -Path $pidFile -Value $process.Id -Encoding Ascii

$deadline = (Get-Date).AddSeconds($WaitSeconds)
$readyUrl = "http://127.0.0.1:$MetricsPort/metrics"

while ((Get-Date) -lt $deadline) {
    if (-not (Test-PidAlive $process.Id)) {
        if (Test-Path $logFile) {
            Get-Content $logFile -Tail 50
        }
        Fail 'Janus exited during startup'
    }
    try {
        Invoke-WebRequest -Uri $readyUrl -UseBasicParsing | Out-Null
        Write-Host "Janus ready. pid=$($process.Id)"
        Write-Host "Log: $logFile"
        exit 0
    }
    catch {
        try {
            Invoke-WebRequest -Uri "http://127.0.0.1:$MetricsPort/" -UseBasicParsing | Out-Null
            Write-Host "Janus ready. pid=$($process.Id)"
            Write-Host "Log: $logFile"
            exit 0
        }
        catch {
            if (Test-Path $logFile) {
                $started = Select-String -Path $logFile -Pattern 'Janus Server started successfully' -Quiet
                if ($started) {
                    Write-Host "Janus ready. pid=$($process.Id)"
                    Write-Host "Log: $logFile"
                    exit 0
                }
            }
            Start-Sleep -Seconds 1
        }
    }
}

if (Test-Path $logFile) {
    Get-Content $logFile -Tail 50
}
Fail "Timed out waiting for $readyUrl"
