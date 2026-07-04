param(
    [switch]$InstallGoTools,
    [switch]$NoSQLite,
    [switch]$Help
)

$ErrorActionPreference = "Continue"

$ProtoGenGoVersion = "v1.36.11"
$ProtoGenGoGrpcVersion = "v1.6.1"
$MageVersion = "v1.17.2"
$MinProtocVersion = "25.0"
$MinNodeVersion = "20.19.0"

function Show-Usage {
    @"
Usage: .\scripts\dev-doctor.ps1 [-InstallGoTools] [-NoSQLite]

Checks the local Vectis development toolchain with friendly install guidance.

Options:
  -InstallGoTools  Install Mage, protoc-gen-go, and protoc-gen-go-grpc with go install.
  -NoSQLite        Skip CGO/C compiler checks for nosqlite development lanes.
  -Help            Show this help.
"@
}

if ($Help) {
    Show-Usage
    exit 0
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Resolve-Path (Join-Path $ScriptDir "..")
$GoMod = Join-Path $RootDir "go.mod"
$MinGo = (Select-String -Path $GoMod -Pattern '^go\s+(.+)$' | Select-Object -First 1).Matches[0].Groups[1].Value

$script:Failures = 0
$script:Warnings = 0

function Section([string]$Title) {
    Write-Host ""
    Write-Host $Title
}

function Pass([string]$Message) {
    Write-Host "  [ok] $Message"
}

function Warn([string]$Message) {
    $script:Warnings++
    Write-Host "  [warn] $Message"
}

function Fail([string]$Message) {
    $script:Failures++
    Write-Host "  [missing] $Message"
}

function Note([string]$Message) {
    Write-Host "          $Message"
}

function Find-CommandPath([string]$Name) {
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $cmd) {
        return $cmd.Source
    }

    return $null
}

function Normalize-Version([string]$Value) {
    if ($Value -match '(\d+(?:\.\d+){0,2})') {
        return $Matches[1]
    }

    return ""
}

function Test-VersionAtLeast([string]$Got, [string]$Want) {
    try {
        $gotVersion = [version](Normalize-Version $Got)
        $wantVersion = [version](Normalize-Version $Want)
        return $gotVersion -ge $wantVersion
    } catch {
        return $false
    }
}

function Check-RequiredCommand([string]$Name, [string]$Purpose, [string[]]$Guidance) {
    $path = Find-CommandPath $Name
    if ($path) {
        Pass "$Name found at $path ($Purpose)"
    } else {
        Fail "$Name not found ($Purpose)"
        foreach ($line in $Guidance) {
            Note $line
        }
    }
}

function Check-OptionalCommand([string]$Name, [string]$Purpose, [string[]]$Guidance) {
    $path = Find-CommandPath $Name
    if ($path) {
        Pass "$Name found at $path ($Purpose)"
    } else {
        Warn "$Name not found ($Purpose)"
        foreach ($line in $Guidance) {
            Note $line
        }
    }
}

function Find-GoTool([string]$Tool) {
    $path = Find-CommandPath $Tool
    if ($path) {
        return $path
    }

    if (Find-CommandPath "go") {
        $gopath = (& go env GOPATH 2>$null).Trim()
        if ($gopath) {
            foreach ($name in @("$Tool.exe", $Tool)) {
                $candidate = Join-Path (Join-Path $gopath "bin") $name
                if (Test-Path $candidate) {
                    return $candidate
                }
            }
        }
    }

    return $null
}

function Find-CCompiler {
    $cc = $null
    if (Find-CommandPath "go") {
        $cc = (& go env CC 2>$null).Trim()
    }

    if ($cc) {
        $ccName = ($cc -split '\s+')[0]
        if (Test-UnsupportedCGOCompiler $ccName) {
            return [pscustomobject]@{
                Name        = "go env CC ($cc)"
                Path        = ""
                Unsupported = $true
            }
        }

        $ccPath = Find-CommandPath $ccName
        if ($ccPath) {
            return [pscustomobject]@{
                Name        = "go env CC ($cc)"
                Path        = $ccPath
                Unsupported = $false
            }
        }
    }

    $candidates = @(
        @{ Name = "clang"; Commands = @("clang.exe", "clang") },
        @{ Name = "gcc"; Commands = @("gcc.exe", "gcc") }
    )

    foreach ($candidate in $candidates) {
        foreach ($command in $candidate.Commands) {
            $path = Find-CommandPath $command
            if ($path) {
                return [pscustomobject]@{
                    Name        = $candidate.Name
                    Path        = $path
                    Unsupported = $false
                }
            }
        }
    }

    return $null
}

function Test-UnsupportedCGOCompiler([string]$Command) {
    if (-not $Command) {
        return $false
    }

    $name = [System.IO.Path]::GetFileNameWithoutExtension($Command).ToLowerInvariant()
    return $name -eq "cl" -or $name -eq "clang-cl"
}

function Test-DirectorySymlink {
    $root = Join-Path ([System.IO.Path]::GetTempPath()) ("vectis-symlink-check-" + [guid]::NewGuid().ToString("N"))
    $target = Join-Path $root "target"
    $link = Join-Path $root "link"

    try {
        New-Item -ItemType Directory -Path $target -Force -ErrorAction Stop | Out-Null
        New-Item -ItemType SymbolicLink -Path $link -Target $target -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $_.Exception.Message
    } finally {
        if (Test-Path -LiteralPath $root) {
            Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

function Install-GoTool([string]$Module) {
    if (-not (Find-CommandPath "go")) {
        Fail "cannot install $Module because go is not on PATH"
        return
    }

    Write-Host "  [run] go install $Module"
    & go install $Module

    if ($LASTEXITCODE -eq 0) {
        Pass "installed $Module"
    } else {
        Fail "go install failed for $Module"
        Note "Check network access and GOPATH/GOBIN permissions, then rerun this script."
    }
}

if ($InstallGoTools) {
    Section "Installing Go Tools"
    Install-GoTool "github.com/magefile/mage@$MageVersion"
    Install-GoTool "google.golang.org/protobuf/cmd/protoc-gen-go@$ProtoGenGoVersion"
    Install-GoTool "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$ProtoGenGoGrpcVersion"
}

Section "Required Tools"

if (Find-CommandPath "go") {
    $goVersionRaw = (& go env GOVERSION 2>$null).Trim()
    if (Test-VersionAtLeast $goVersionRaw $MinGo) {
        Pass "go $goVersionRaw satisfies go.mod requirement $MinGo+"
    } else {
        Fail "go $goVersionRaw is older than go.mod requirement $MinGo"
        Note "Install Go $MinGo+ from https://go.dev/doc/install."
        Note "Windows: winget install GoLang.Go"
    }
} else {
    Fail "go not found"
    Note "Install Go $MinGo+ from https://go.dev/doc/install."
    Note "Windows: winget install GoLang.Go"
}

Check-RequiredCommand "git" "source control and ci-quick worktree checks" @(
    "Install Git from https://git-scm.com/downloads.",
    "Windows: winget install Git.Git"
)

$mage = Find-GoTool "mage"
if ($mage) {
    $version = (& $mage --version 2>$null | Select-Object -First 1) -join " "
    $versionSuffix = ""
    if ($version) {
        $versionSuffix = " ($version)"
    }

    Pass "mage found at $mage$versionSuffix"
} else {
    Fail "mage not found"
    Note "Run: .\scripts\dev-doctor.ps1 -InstallGoTools"
    Note "Or: go install github.com/magefile/mage@$MageVersion"
}

$protocPath = Find-CommandPath "protoc"
if ($protocPath) {
    $protocRaw = (& protoc --version 2>$null) -join " "
    if (Test-VersionAtLeast $protocRaw $MinProtocVersion) {
        Pass "protoc $protocRaw satisfies protobuf edition 2023 requirement $MinProtocVersion+"
    } else {
        Fail "protoc $protocRaw is older than required $MinProtocVersion+"
        Note "api/proto uses edition = `"2023`"; older protoc builds parse it as proto2."
        Note "Install protoc $MinProtocVersion+ from https://protobuf.dev/installation/."
        Note "Windows: download a protoc zip from https://github.com/protocolbuffers/protobuf/releases and add its bin directory to PATH."
        Note "Scoop: scoop install protobuf. Chocolatey: choco install protoc."
    }
} else {
    Fail "protoc not found (protobuf regeneration with mage proto)"
    Note "Install protoc $MinProtocVersion+ from https://protobuf.dev/installation/."
    Note "Windows: download a protoc zip from https://github.com/protocolbuffers/protobuf/releases and add its bin directory to PATH."
    Note "Scoop: scoop install protobuf. Chocolatey: choco install protoc."
}

$protocGenGo = Find-GoTool "protoc-gen-go"
if ($protocGenGo) {
    $version = (& $protocGenGo --version 2>$null) -join " "
    $versionSuffix = ""
    if ($version) {
        $versionSuffix = " ($version)"
    }

    Pass "protoc-gen-go found at $protocGenGo$versionSuffix"
} else {
    Fail "protoc-gen-go not found"
    Note "Run: .\scripts\dev-doctor.ps1 -InstallGoTools"
    Note "Or: go install google.golang.org/protobuf/cmd/protoc-gen-go@$ProtoGenGoVersion"
}

$protocGenGoGrpc = Find-GoTool "protoc-gen-go-grpc"
if ($protocGenGoGrpc) {
    $version = (& $protocGenGoGrpc --version 2>$null) -join " "
    $versionSuffix = ""

    if ($version) {
        $versionSuffix = " ($version)"
    }

    Pass "protoc-gen-go-grpc found at $protocGenGoGrpc$versionSuffix"
} else {
    Fail "protoc-gen-go-grpc not found"
    Note "Run: .\scripts\dev-doctor.ps1 -InstallGoTools"
    Note "Or: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$ProtoGenGoGrpcVersion"
}

Section "Frontend Lane"
$nodePath = Find-CommandPath "node"
if ($nodePath) {
    $nodeRaw = (& node --version 2>$null).Trim()

    if (Test-VersionAtLeast $nodeRaw $MinNodeVersion) {
        Pass "node $nodeRaw satisfies docs/UI build requirement $MinNodeVersion+"
    } else {
        Warn "node $nodeRaw is older than docs/UI build requirement $MinNodeVersion+"
        Note "Install Node.js $MinNodeVersion+ from https://nodejs.org/."
        Note "Windows: winget install OpenJS.NodeJS.LTS"
    }
} else {
    Warn "node not found (required only for docs/UI targets such as mage buildFrontend)"
    Note "Install Node.js $MinNodeVersion+ from https://nodejs.org/."
    Note "Windows: winget install OpenJS.NodeJS.LTS"
}

Check-OptionalCommand "npm" "docs/UI dependency installation and frontend builds" @(
    "npm ships with Node.js. Install Node.js $MinNodeVersion+ from https://nodejs.org/.",
    "Windows: winget install OpenJS.NodeJS.LTS"
)

Section "SQLite / CGO"
if ($NoSQLite) {
    Warn "skipping CGO/C compiler checks because -NoSQLite was supplied"
} elseif (Find-CommandPath "go") {
    $cgoEnabled = (& go env CGO_ENABLED 2>$null).Trim()
    if ($cgoEnabled -eq "1") {
        Pass "CGO_ENABLED=1"
    } else {
        Fail "CGO_ENABLED=$cgoEnabled, but local SQLite development needs CGO_ENABLED=1"
        Note "Unset CGO_ENABLED or set CGO_ENABLED=1. Use -NoSQLite for nosqlite build lanes."
    }

    $compiler = Find-CCompiler

    if ($compiler -and $compiler.Unsupported) {
        Fail ("unsupported C compiler for SQLite CGO builds: {0}" -f $compiler.Name)
        Note "Go cgo and github.com/mattn/go-sqlite3 use GCC/Clang-style compiler flags."
        Note "Windows: install MSYS2 (winget install MSYS2.MSYS2), then install a MinGW/UCRT GCC toolchain from an MSYS2 shell."
        Note "Windows: LLVM clang in GCC-compatible mode is also supported; MSVC cl.exe and clang-cl are not supported for this lane."
    } elseif ($compiler) {
        Pass ("C compiler found: {0} at {1}" -f $compiler.Name, $compiler.Path)
    } else {
        Fail "no C compiler found for CGO SQLite builds"
        Note "Windows: install MSYS2 (winget install MSYS2.MSYS2), then install a MinGW/UCRT GCC toolchain from an MSYS2 shell."
        Note "Windows: LLVM clang in GCC-compatible mode is also supported; MSVC cl.exe and clang-cl are not supported for this lane."
        Note "Alternative: use -NoSQLite for nosqlite build lanes until native SQLite CGO is configured."
    }
}

Section "Windows Filesystem"
$symlinkCheck = Test-DirectorySymlink
if ($symlinkCheck -eq $true) {
    Pass "directory symlinks are available"
} else {
    Warn "directory symlinks are not available for this shell"
    Note "Enable Windows Developer Mode or run from an elevated shell to exercise checkout-cache symlink tests."
    Note ("Symlink probe error: {0}" -f $symlinkCheck)
}

Section "Optional Lanes"
Check-OptionalCommand "podman" "container image targets" @(
    "Install Podman from https://podman.io/docs/installation/ when working on image-* targets.",
    "Windows: winget install RedHat.Podman"
)

Check-OptionalCommand "packer" "VM/package e2e preparation" @(
    "Install Packer from https://developer.hashicorp.com/packer/install when working on VM/package lanes.",
    "Windows: winget install Hashicorp.Packer"
)

Check-OptionalCommand "java" "formal-verification TLA+ target" @(
    "Install a JDK and set TLA_TOOLS_JAR if you need formal-verification.",
    "Windows: winget install EclipseAdoptium.Temurin.21.JDK"
)

$tlaJar = $env:TLA_TOOLS_JAR
if (-not $tlaJar) {
    $tlaJar = "/opt/tla+/tla2tools.jar"
}

if (Test-Path $tlaJar) {
    Pass "TLA+ tools jar found at $tlaJar"
} else {
    Warn "TLA+ tools jar not found at $tlaJar"
    Note "Set TLA_TOOLS_JAR to the tla2tools.jar path when using formal-verification targets."
}

Section "Summary"
if ($script:Failures -gt 0) {
    Write-Host ("  {0} required check(s) failed; {1} warning(s)." -f $script:Failures, $script:Warnings)
    Write-Host "  Fix the missing required tools above, then rerun .\scripts\dev-doctor.ps1."
    exit 1
}

Write-Host ("  All required checks passed; {0} warning(s)." -f $script:Warnings)
