#Requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateSet("build", "start", "stop", "test", "benchmark")]
    [string] $Action = "start",
    [ValidateRange(1, 2000000)]
    [int] $Rows = 10000,
    [ValidateRange(0, 100)]
    [int] $Warmups = 2,
    [ValidateRange(1, 1000)]
    [int] $Iterations = 5,
    [string] $Output
)

if ($Action -eq "benchmark" -and -not $PSBoundParameters.ContainsKey("Rows")) {
    $Rows = 100000
}

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$envFile = Join-Path $PSScriptRoot ".env"
$image = "kola-kdb-test:local"
$container = "kola-kdb-test"

function Import-KdbEnvironment {
    param([Parameter(Mandatory)] [string[]] $Names)
    if (-not (Test-Path $envFile)) {
        return
    }

    foreach ($line in Get-Content $envFile) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        $parts = $trimmed.Split("=", 2)
        if ($parts.Count -ne 2 -or -not $parts[0]) {
            throw "Invalid entry in $envFile"
        }
        if ($Names -notcontains $parts[0]) {
            continue
        }

        if (-not [Environment]::GetEnvironmentVariable($parts[0], "Process")) {
            [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
        }
    }
}

function Assert-KdbEnvironment {
    param([Parameter(Mandatory)] [string[]] $Names)

    foreach ($name in $Names) {
        if (-not [Environment]::GetEnvironmentVariable($name, "Process")) {
            throw "$name must be set in the process environment or $envFile"
        }
    }
}

function Invoke-Podman {
    param([Parameter(ValueFromRemainingArguments)] [string[]] $Arguments)

    & podman @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "podman command failed with exit code $LASTEXITCODE"
    }
}

function Convert-ToPodmanMachinePath {
    param([Parameter(Mandatory)] [string] $Path)

    $fullPath = [IO.Path]::GetFullPath($Path)
    if ($fullPath -notmatch "^([A-Za-z]):\\(.*)$") {
        throw "Cannot map path into the Podman machine: $fullPath"
    }

    $drive = $Matches[1].ToLowerInvariant()
    $relativePath = $Matches[2].Replace("\", "/")
    return "/mnt/$drive/$relativePath"
}

function Convert-ToPosixShellLiteral {
    param([Parameter(Mandatory)] [string] $Value)

    $singleQuoteEscape = "'" + '"' + "'" + '"' + "'"
    return "'" + $Value.Replace("'", $singleQuoteEscape) + "'"
}

function Build-KdbImage {
    Import-KdbEnvironment "KX_BEARER_TOKEN", "KDB_LICENSE_B64"
    Assert-KdbEnvironment "KX_BEARER_TOKEN", "KDB_LICENSE_B64"

    $tokenFile = $null
    $licenseFile = $null
    $buildFailure = $null
    try {
        $tokenFile = [IO.Path]::GetTempFileName()
        $licenseFile = [IO.Path]::GetTempFileName()
        [IO.File]::WriteAllText(
            $tokenFile,
            [Environment]::GetEnvironmentVariable("KX_BEARER_TOKEN", "Process")
        )
        [IO.File]::WriteAllText(
            $licenseFile,
            [Environment]::GetEnvironmentVariable("KDB_LICENSE_B64", "Process")
        )

        $machineRoot = Convert-ToPodmanMachinePath $repoRoot
        $machineToken = Convert-ToPodmanMachinePath $tokenFile
        $machineLicense = Convert-ToPodmanMachinePath $licenseFile
        $remoteCommand = "podman build --format docker " +
            "--secret id=kx-token,src=$(Convert-ToPosixShellLiteral $machineToken) " +
            "--secret id=kx-license,src=$(Convert-ToPosixShellLiteral $machineLicense) " +
            "--tag $image " +
            "--file $(Convert-ToPosixShellLiteral "$machineRoot/testing/kdb/Containerfile") " +
            "$(Convert-ToPosixShellLiteral "$machineRoot/testing/kdb")"
        Invoke-Podman machine ssh -- $remoteCommand
    }
    catch {
        $buildFailure = $_
    }

    $cleanupFailures = @()
    foreach ($secretFile in $tokenFile, $licenseFile) {
        if (-not $secretFile) {
            continue
        }
        try {
            Remove-Item -Force -ErrorAction Stop $secretFile
            if (Test-Path $secretFile) {
                throw "file still exists after removal"
            }
        }
        catch {
            $cleanupFailures += "${secretFile}: $($_.Exception.Message)"
        }
    }

    if ($buildFailure) {
        if ($cleanupFailures.Count -gt 0) {
            throw "$($buildFailure.Exception.Message); secret cleanup failed: $($cleanupFailures -join '; ')"
        }
        throw $buildFailure
    }
    if ($cleanupFailures.Count -gt 0) {
        throw "Secret cleanup failed: $($cleanupFailures -join '; ')"
    }
}

function Stop-KdbContainer {
    try {
        Invoke-Podman rm --force --ignore $container
    }
    finally {
        Invoke-Podman secret rm --ignore kola-kdb-license
    }
}

function Start-KdbContainer {
    Import-KdbEnvironment "KDB_LICENSE_B64"
    Assert-KdbEnvironment "KDB_LICENSE_B64"

    & podman image exists $image
    if ($LASTEXITCODE -ne 0) {
        Build-KdbImage
    }

    Stop-KdbContainer
    Invoke-Podman secret create --replace --env=true kola-kdb-license KDB_LICENSE_B64
    try {
        Invoke-Podman run --detach `
            --name $container `
            --publish 127.0.0.1:1801:1801/tcp `
            --cpus 4 `
            --memory 4g `
            --tmpfs "/run/kdb-license:rw,nosuid,nodev,noexec,size=1m" `
            --env "KOLA_Q_ROWS=$Rows" `
            --secret "kola-kdb-license,target=kdb-license-b64,type=mount" `
            $image

        foreach ($attempt in 1..30) {
            $null = & podman healthcheck run $container 2>$null
            if ($LASTEXITCODE -eq 0) {
                return
            }
            Start-Sleep -Seconds 1
        }

        & podman logs $container
        throw "q did not become healthy within 30 seconds"
    }
    catch {
        Stop-KdbContainer
        throw
    }
}

function Initialize-PythonEnvironment {
    [Environment]::SetEnvironmentVariable("KX_BEARER_TOKEN", $null, "Process")
    [Environment]::SetEnvironmentVariable("KDB_LICENSE_B64", $null, "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_EXTERNAL", "1", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_HOST", "127.0.0.1", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_PORT", "1801", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_Q_ROWS", "$Rows", "Process")

    $venv = Join-Path $repoRoot ".venv"
    $python = Join-Path $venv "Scripts/python.exe"
    if (-not (Test-Path $python)) {
        & uv venv --python 3.12 $venv
        if ($LASTEXITCODE -ne 0) {
            throw "uv venv failed with exit code $LASTEXITCODE"
        }
    }

    & uv pip install --python $python -r (Join-Path $repoRoot "py-kola/requirements.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "uv pip install failed with exit code $LASTEXITCODE"
    }

    [Environment]::SetEnvironmentVariable("VIRTUAL_ENV", $venv, "Process")
    $maturin = Join-Path $venv "Scripts/maturin.exe"
    Push-Location $repoRoot
    try {
        & $maturin develop --manifest-path py-kola/Cargo.toml
        if ($LASTEXITCODE -ne 0) {
            throw "maturin develop failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

function Test-KdbContainer {
    Start-KdbContainer
    try {
        Initialize-PythonEnvironment
        $python = Join-Path $repoRoot ".venv/Scripts/python.exe"
        Push-Location $repoRoot
        try {
            & $python -m pytest -q py-kola/test
            if ($LASTEXITCODE -ne 0) {
                throw "pytest failed with exit code $LASTEXITCODE"
            }
        }
        finally {
            Pop-Location
        }
    }
    finally {
        Stop-KdbContainer
    }
}

function Benchmark-KdbContainer {
    Start-KdbContainer
    try {
        Initialize-PythonEnvironment
        $python = Join-Path $repoRoot ".venv/Scripts/python.exe"
        $arguments = @(
            "py-kola/benchmarks/bench_ipc.py",
            "--warmups", "$Warmups",
            "--iterations", "$Iterations"
        )
        if ($Output) {
            $outputPath = [IO.Path]::GetFullPath($Output, (Get-Location).Path)
            $arguments += "--output", $outputPath
        }
        Push-Location $repoRoot
        try {
            & $python @arguments
            if ($LASTEXITCODE -ne 0) {
                throw "benchmark failed with exit code $LASTEXITCODE"
            }
        }
        finally {
            Pop-Location
        }
    }
    finally {
        Stop-KdbContainer
    }
}

$managedEnvironment = @(
    "KX_BEARER_TOKEN",
    "KDB_LICENSE_B64",
    "KOLA_TEST_Q_EXTERNAL",
    "KOLA_TEST_Q_HOST",
    "KOLA_TEST_Q_PORT",
    "KOLA_Q_ROWS",
    "VIRTUAL_ENV"
)
$savedEnvironment = @{}
foreach ($name in $managedEnvironment) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

try {
    switch ($Action) {
        "build" { Build-KdbImage }
        "start" { Start-KdbContainer }
        "stop" { Stop-KdbContainer }
        "test" { Test-KdbContainer }
        "benchmark" { Benchmark-KdbContainer }
    }
}
finally {
    foreach ($name in $managedEnvironment) {
        [Environment]::SetEnvironmentVariable(
            $name,
            $savedEnvironment[$name],
            "Process"
        )
    }
}
