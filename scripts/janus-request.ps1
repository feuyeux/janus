[CmdletBinding()]
param(
    [string]$Url = 'ws://127.0.0.1:8080/json',
    [ValidateRange(1, 86400)]
    [int]$DurationSeconds = 70,
    [ValidateRange(1, 512)]
    [int]$Parallelism = 4,
    [ValidateRange(0, 60000)]
    [int]$PauseMillis = 0
)

$ErrorActionPreference = 'Stop'

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

$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$javaCmd = Get-JavaCommand

Push-Location $projectRoot
try {
    & $javaCmd scripts/JanusWsLoad.java --url $Url --duration-seconds $DurationSeconds --parallelism $Parallelism --pause-millis $PauseMillis
    if ($LASTEXITCODE -ne 0) {
        Fail 'Janus request load failed'
    }
}
finally {
    Pop-Location
}
