[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [Alias('Pid')]
    [int]$TargetPid = 0,
    [string]$Matcher = 'org\.janus\.JanusServer|janus-server-java|janus\.jar',
    [ValidateRange(1, 86400)]
    [int]$CpuDuration = 30,
    [ValidateRange(1, 86400)]
    [int]$MemDuration = 30,
    [string]$MemEvent = 'alloc',
    [switch]$Live,
    [string]$OutputDir = (Join-Path $PSScriptRoot '..' 'arthas-output'),
    [string]$ArthasJar = (Join-Path $PSScriptRoot '..' '.tools' 'arthas' 'arthas-boot.jar'),
    [switch]$SkipCpu,
    [switch]$SkipMem
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

function Ensure-ArthasJar([string]$JarPath) {
    if (Test-Path $JarPath) {
        return
    }

    $jarDir = Split-Path -Parent $JarPath
    New-Item -ItemType Directory -Path $jarDir -Force | Out-Null
    Write-Host "Downloading Arthas bootstrap to $JarPath"
    Invoke-WebRequest -Uri 'https://arthas.aliyun.com/arthas-boot.jar' -OutFile $JarPath
}

function Get-JavaProcessLines {
    $jcmd = Get-Command jcmd -ErrorAction SilentlyContinue
    if ($jcmd) {
        return & $jcmd.Source -l 2>$null
    }

    $jps = Get-Command jps -ErrorAction SilentlyContinue
    if ($jps) {
        return & $jps.Source -l 2>$null
    }

    Fail 'Neither jcmd nor jps is available. Please use a full JDK or pass -Pid.'
}

function Resolve-TargetPid {
    param(
        [int]$ExplicitPid,
        [string]$Pattern
    )

    if ($ExplicitPid -gt 0) {
        return $ExplicitPid
    }

    $lines = @(Get-JavaProcessLines)
    $matches = @($lines | Where-Object { $_ -match $Pattern })

    if ($matches.Count -eq 0) {
        Write-Host 'Visible JVM processes:'
        $lines | ForEach-Object { Write-Host $_ }
        Fail "No Janus JVM matched regex: $Pattern"
    }

    if ($matches.Count -gt 1) {
        Write-Host 'Multiple JVM processes matched. Please rerun with -Pid.'
        $matches | ForEach-Object { Write-Host $_ }
        Fail 'Ambiguous target JVM'
    }

    $pidText = ($matches[0] -split '\s+')[0]
    $parsedPid = 0
    if (-not [int]::TryParse($pidText, [ref]$parsedPid)) {
        Fail "Unable to parse pid from process line: $($matches[0])"
    }
    return $parsedPid
}

function Invoke-ArthasProfile {
    param(
        [string]$JavaCmd,
        [string]$JarPath,
        [int]$TargetPid,
        [string]$Event,
        [int]$Duration,
        [string]$OutputFile,
        [bool]$UseLive
    )

    $startCommand = 'profiler start --event {0}' -f $Event
    if ($UseLive) {
        $startCommand += ' --live'
    }
    $stopCommand = 'profiler stop --file "{0}"' -f $OutputFile

    try {
        Write-Host "Generating $Event flame graph -> $OutputFile"
        & $JavaCmd -jar $JarPath -c $startCommand $TargetPid
        if ($LASTEXITCODE -ne 0) {
            Fail "Arthas profiler start failed for event: $Event"
        }
        Start-Sleep -Seconds $Duration
        & $JavaCmd -jar $JarPath -c $stopCommand $TargetPid
        if ($LASTEXITCODE -ne 0) {
            Fail "Arthas profiler stop failed for event: $Event"
        }
    }
    finally {
    }
}

if ($SkipCpu -and $SkipMem) {
    Fail 'Cannot skip both cpu and memory profiling'
}

$javaCmd = Get-JavaCommand
Ensure-ArthasJar -JarPath $ArthasJar
$targetPid = Resolve-TargetPid -ExplicitPid $TargetPid -Pattern $Matcher

$resolvedOutputDir = [System.IO.Path]::GetFullPath($OutputDir)
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runDir = Join-Path $resolvedOutputDir "$timestamp-pid-$targetPid"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

if (-not $SkipCpu) {
    $cpuOut = Join-Path $runDir ("cpu-{0}s.html" -f $CpuDuration)
    Invoke-ArthasProfile -JavaCmd $javaCmd -JarPath $ArthasJar -TargetPid $targetPid -Event 'cpu' -Duration $CpuDuration -OutputFile $cpuOut -UseLive:$false
}

if (-not $SkipMem) {
    $memOut = Join-Path $runDir ("memory-{0}-{1}s.html" -f $MemEvent, $MemDuration)
    Invoke-ArthasProfile -JavaCmd $javaCmd -JarPath $ArthasJar -TargetPid $targetPid -Event $MemEvent -Duration $MemDuration -OutputFile $memOut -UseLive:$Live.IsPresent
}

Write-Host "Done. Flame graphs saved under: $runDir"
