#Requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateSet("build", "start", "stop", "test", "benchmark", "compare", "test-node", "benchmark-node")]
    [string] $Action = "start",
    [ValidateRange(1, 2000000)]
    [int] $Rows = 10000,
    [ValidateRange(0, 100)]
    [int] $Warmups = 2,
    [ValidateRange(1, 1000)]
    [int] $Iterations = 5,
    [string] $Output,
    # Opt-in: run the q container with host networking. Unlike the default loopback-only
    # port publish, this binds the unauthenticated q listener on the podman machine's
    # interfaces (machine-local peers can reach it; with WSL mirrored networking it may be
    # reachable from the LAN). Only for machines whose netavark cannot program port rules.
    [switch] $HostNetwork
)

$benchmarkActions = @("benchmark", "compare", "benchmark-node")
if ($benchmarkActions -contains $Action -and -not $PSBoundParameters.ContainsKey("Rows")) {
    $Rows = 100000
}

if ($benchmarkActions -contains $Action -and -not $PSBoundParameters.ContainsKey("Iterations")) {
    $Iterations = 100
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

    $secretDirectory = $null
    $buildFailure = $null
    try {
        $candidateDirectory = Join-Path ([IO.Path]::GetTempPath()) (
            "kola-kdb-" + [Guid]::NewGuid().ToString("N")
        )
        New-Item -ItemType Directory -Path $candidateDirectory -ErrorAction Stop | Out-Null
        $secretDirectory = $candidateDirectory

        $currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
        $inheritanceFlags = [Security.AccessControl.InheritanceFlags]"ContainerInherit, ObjectInherit"
        $secretAcl = [Security.AccessControl.DirectorySecurity]::new()
        $secretAcl.SetOwner($currentSid)
        $secretAcl.SetAccessRuleProtection($true, $false)
        $secretAcl.AddAccessRule(
            [Security.AccessControl.FileSystemAccessRule]::new(
                $currentSid,
                [Security.AccessControl.FileSystemRights]::FullControl,
                $inheritanceFlags,
                [Security.AccessControl.PropagationFlags]::None,
                [Security.AccessControl.AccessControlType]::Allow
            )
        )
        Set-Acl -LiteralPath $secretDirectory -AclObject $secretAcl -ErrorAction Stop

        $appliedAcl = Get-Acl -LiteralPath $secretDirectory -ErrorAction Stop
        $appliedRules = @(
            $appliedAcl.GetAccessRules(
                $true,
                $true,
                [Security.Principal.SecurityIdentifier]
            )
        )
        if (
            -not $appliedAcl.AreAccessRulesProtected -or
            $appliedAcl.GetOwner([Security.Principal.SecurityIdentifier]).Value -ne $currentSid.Value -or
            $appliedRules.Count -ne 1 -or
            $appliedRules[0].IdentityReference.Value -ne $currentSid.Value -or
            $appliedRules[0].AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow -or
            $appliedRules[0].FileSystemRights -ne [Security.AccessControl.FileSystemRights]::FullControl -or
            $appliedRules[0].InheritanceFlags -ne $inheritanceFlags -or
            $appliedRules[0].PropagationFlags -ne [Security.AccessControl.PropagationFlags]::None -or
            $appliedRules[0].IsInherited
        ) {
            throw "Secret staging directory ACL verification failed"
        }

        $tokenFile = Join-Path $secretDirectory ("kx-token-" + [Guid]::NewGuid().ToString("N"))
        $licenseFile = Join-Path $secretDirectory ("kdb-license-" + [Guid]::NewGuid().ToString("N"))
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
    if ($secretDirectory) {
        try {
            Remove-Item -LiteralPath $secretDirectory -Recurse -Force -ErrorAction Stop
            if (Test-Path -LiteralPath $secretDirectory) {
                throw "directory still exists after removal"
            }
        }
        catch {
            $cleanupFailures += "${secretDirectory}: $($_.Exception.Message)"
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

function Start-KdbContainerOnce {
    param([Parameter(Mandatory)] [bool] $UseHostNetwork)

    $networkArguments = if ($UseHostNetwork) {
        @("--network", "host")
    } else {
        @("--publish", "127.0.0.1:1801:1801/tcp")
    }
    Invoke-Podman run --detach `
        --name $container `
        @networkArguments `
        --cpus 4 `
        --memory 4g `
        --tmpfs "/run/kdb-license:rw,nosuid,nodev,noexec,size=1m" `
        --env "KOLA_Q_ROWS=$Rows" `
        --secret "kola-kdb-license,target=kdb-license-b64,type=mount" `
        $image
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
        if ($HostNetwork) {
            Write-Warning "Host networking requested: the unauthenticated q listener is not loopback-restricted."
            Start-KdbContainerOnce -UseHostNetwork $true
        } else {
            try {
                Start-KdbContainerOnce -UseHostNetwork $false
            }
            catch {
                Invoke-Podman rm --force --ignore $container
                throw ("Loopback port publish failed ($($_.Exception.Message)). " +
                    "If this machine's netavark cannot program port rules, rerun with " +
                    "-HostNetwork to accept a non-loopback-restricted q listener.")
            }
        }

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

    $pyprojectPath = Join-Path $repoRoot "py-kola/pyproject.toml"
    & uv pip install --python $python `
        --group "${pyprojectPath}:dev" `
        --group "${pyprojectPath}:benchmark"
    if ($LASTEXITCODE -ne 0) {
        throw "uv pip install failed with exit code $LASTEXITCODE"
    }

    [Environment]::SetEnvironmentVariable("VIRTUAL_ENV", $venv, "Process")
    $maturin = Join-Path $venv "Scripts/maturin.exe"
    # maturin resolves dependency-group names against its working directory,
    # so it must run from the package directory.
    Push-Location (Join-Path $repoRoot "py-kola")
    try {
        & $maturin develop --release
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

function Compare-KdbContainer {
    Start-KdbContainer
    try {
        [Environment]::SetEnvironmentVariable("KX_BEARER_TOKEN", $null, "Process")
        [Environment]::SetEnvironmentVariable("KDB_LICENSE_B64", $null, "Process")
        [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_HOST", "127.0.0.1", "Process")
        [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_PORT", "1801", "Process")
        $arguments = @(
            "run", "--no-project", "--python", "3.12", "python",
            "py-kola/benchmarks/compare_upstream.py",
            "--warmups", "$Warmups",
            "--iterations", "$Iterations"
        )
        if ($Output) {
            $outputPath = [IO.Path]::GetFullPath($Output, (Get-Location).Path)
            $arguments += "--output", $outputPath
        }
        Push-Location $repoRoot
        try {
            & uv @arguments
            if ($LASTEXITCODE -ne 0) {
                throw "upstream comparison failed with exit code $LASTEXITCODE"
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

function Initialize-NodeEnvironment {
    [Environment]::SetEnvironmentVariable("KX_BEARER_TOKEN", $null, "Process")
    [Environment]::SetEnvironmentVariable("KDB_LICENSE_B64", $null, "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_EXTERNAL", "1", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_HOST", "127.0.0.1", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_TEST_Q_PORT", "1801", "Process")
    [Environment]::SetEnvironmentVariable("KOLA_Q_ROWS", "$Rows", "Process")

    Push-Location (Join-Path $repoRoot "js-kola")
    try {
        & npm install --no-audit --no-fund
        if ($LASTEXITCODE -ne 0) {
            throw "npm install failed with exit code $LASTEXITCODE"
        }

        & npm run build:release
        if ($LASTEXITCODE -ne 0) {
            throw "Node release build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

function Test-NodeKdbContainer {
    Start-KdbContainer
    try {
        Initialize-NodeEnvironment
        Push-Location (Join-Path $repoRoot "js-kola")
        try {
            & npm run test:live
            if ($LASTEXITCODE -ne 0) {
                throw "live Node tests failed with exit code $LASTEXITCODE"
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

function Benchmark-NodeKdbContainer {
    Start-KdbContainer
    try {
        Initialize-NodeEnvironment
        $arguments = @(
            "run", "benchmark", "--",
            "--rows", "$Rows",
            "--warmups", "$Warmups",
            "--iterations", "$Iterations"
        )
        if ($Output) {
            $outputPath = [IO.Path]::GetFullPath($Output, (Get-Location).Path)
            $arguments += "--output", $outputPath
        }
        Push-Location (Join-Path $repoRoot "js-kola")
        try {
            & npm @arguments
            if ($LASTEXITCODE -ne 0) {
                throw "Node comparison benchmark failed with exit code $LASTEXITCODE"
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
        "compare" { Compare-KdbContainer }
        "test-node" { Test-NodeKdbContainer }
        "benchmark-node" { Benchmark-NodeKdbContainer }
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
