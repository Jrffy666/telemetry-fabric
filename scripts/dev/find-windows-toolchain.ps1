[CmdletBinding()]
param(
    [switch]$AsMakeArgs
)

$ErrorActionPreference = "Stop"

function Write-Section {
    param([string]$Name)
    Write-Host ""
    Write-Host "== $Name =="
}

function Get-Where {
    param([string]$Name)
    $oldErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $items = & where.exe $Name 2>$null
        if ($LASTEXITCODE -eq 0) {
            $items | ForEach-Object {
                [pscustomobject]@{
                    Tool = $Name
                    Source = "where"
                    Path = $_
                }
            }
        }
    } finally {
        $ErrorActionPreference = $oldErrorActionPreference
    }
}

function Get-CommandPath {
    param([string]$Name)
    Get-Command $Name -ErrorAction SilentlyContinue | ForEach-Object {
        [pscustomobject]@{
            Tool = $Name
            Source = "Get-Command"
            Path = $_.Source
        }
    }
}

function Add-Existing {
    param(
        [string]$Tool,
        [string]$Source,
        [string[]]$Paths
    )
    foreach ($path in $Paths) {
        if ($path -and (Test-Path -LiteralPath $path)) {
            [pscustomobject]@{
                Tool = $Tool
                Source = $Source
                Path = (Resolve-Path -LiteralPath $path).Path
            }
        }
    }
}

function Convert-ToMakePath {
    param([string]$Path)
    $Path -replace "\\", "/"
}

$tools = @("python", "py", "cmake", "ctest", "cl", "gcc", "g++", "clang", "clang++")
$candidates = @()
$candidates += foreach ($tool in $tools) {
    Get-CommandPath $tool
    Get-Where $tool
}

$pythonRoots = @(
    "$env:LOCALAPPDATA\Programs\Python",
    "$env:ProgramFiles\Python",
    "${env:ProgramFiles(x86)}\Python"
) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }

foreach ($root in $pythonRoots) {
    $candidates += Get-ChildItem -Path $root -Recurse -Filter python.exe -File -ErrorAction SilentlyContinue |
        ForEach-Object {
            [pscustomobject]@{
                Tool = "python"
                Source = "common install"
                Path = $_.FullName
            }
        }
}

$candidates += Add-Existing "python" "Windows app alias" @("$env:LOCALAPPDATA\Microsoft\WindowsApps\python.exe")
$candidates += Add-Existing "py" "launcher" @("$env:WINDIR\py.exe", "$env:LOCALAPPDATA\Programs\Python\Launcher\py.exe")

$vswhereCandidates = @(
    "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe",
    "$env:ProgramFiles\Microsoft Visual Studio\Installer\vswhere.exe"
)
$vsInstallations = foreach ($vswhere in $vswhereCandidates) {
    if (Test-Path -LiteralPath $vswhere) {
        & $vswhere -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
    }
}

$knownVsRoots = @(
    "$env:ProgramFiles\Microsoft Visual Studio\2022\BuildTools",
    "$env:ProgramFiles\Microsoft Visual Studio\2022\Community",
    "$env:ProgramFiles\Microsoft Visual Studio\2022\Professional",
    "$env:ProgramFiles\Microsoft Visual Studio\2022\Enterprise"
) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }

$vsRoots = @(@($vsInstallations) + @($knownVsRoots)) | Where-Object { $_ } | Sort-Object -Unique

foreach ($vsRoot in $vsRoots) {
    $vcvars64 = Join-Path $vsRoot "VC\Auxiliary\Build\vcvars64.bat"
    $candidates += Add-Existing "vcvars64" "Visual Studio" @($vcvars64)

    $msvcRoot = Join-Path $vsRoot "VC\Tools\MSVC"
    if (Test-Path -LiteralPath $msvcRoot) {
        $candidates += Get-ChildItem -Path $msvcRoot -Recurse -Filter cl.exe -File -ErrorAction SilentlyContinue |
            ForEach-Object {
                [pscustomobject]@{
                    Tool = "cl"
                    Source = "Visual Studio"
                    Path = $_.FullName
                }
            }
    }

    $vsCmakeRoot = Join-Path $vsRoot "Common7\IDE\CommonExtensions\Microsoft\CMake"
    if (Test-Path -LiteralPath $vsCmakeRoot) {
        $candidates += Get-ChildItem -Path $vsCmakeRoot -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -in @("cmake.exe", "ctest.exe", "ninja.exe") } |
            ForEach-Object {
                [pscustomobject]@{
                    Tool = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)
                    Source = "Visual Studio CMake"
                    Path = $_.FullName
                }
            }
    }
}

$candidates += Add-Existing "cmake" "common install" @(
    "$env:ProgramFiles\CMake\bin\cmake.exe",
    "${env:ProgramFiles(x86)}\CMake\bin\cmake.exe"
)
$candidates += Add-Existing "ctest" "common install" @(
    "$env:ProgramFiles\CMake\bin\ctest.exe",
    "${env:ProgramFiles(x86)}\CMake\bin\ctest.exe"
)
$candidates += Add-Existing "gcc" "MSYS2" @("C:\msys64\mingw64\bin\gcc.exe", "C:\msys64\ucrt64\bin\gcc.exe")
$candidates += Add-Existing "g++" "MSYS2" @("C:\msys64\mingw64\bin\g++.exe", "C:\msys64\ucrt64\bin\g++.exe")
$candidates += Add-Existing "clang" "LLVM" @("$env:ProgramFiles\LLVM\bin\clang.exe")
$candidates += Add-Existing "clang++" "LLVM" @("$env:ProgramFiles\LLVM\bin\clang++.exe")

$all = @($candidates) | Where-Object { $_ } | Sort-Object Tool, Path -Unique

if ($AsMakeArgs) {
    $python = $all | Where-Object { $_.Tool -eq "python" -and $_.Path -notlike "*\Microsoft\WindowsApps\python.exe" } | Select-Object -First 1
    $cmake = $all | Where-Object { $_.Tool -eq "cmake" } | Select-Object -First 1
    $ctest = $all | Where-Object { $_.Tool -eq "ctest" } | Select-Object -First 1

    $args = @()
    if ($python) { $args += "PYTHON=`"$(Convert-ToMakePath $python.Path)`"" }
    if ($cmake) { $args += "CMAKE=`"$(Convert-ToMakePath $cmake.Path)`"" }
    if ($ctest) { $args += "CTEST=`"$(Convert-ToMakePath $ctest.Path)`"" }
    if ($all | Where-Object { $_.Tool -eq "cl" }) {
        $args += "CMAKE_GENERATOR=`"Visual Studio 17 2022`""
        $args += "CMAKE_ARCH=`"x64`""
    }
    $args -join " "
    return
}

Write-Section "PATH and common installations"
$all | Sort-Object Tool, Source, Path | Format-Table -AutoSize

Write-Section "Suggested Make overrides"
& $PSCommandPath -AsMakeArgs

Write-Section "Notes"
Write-Host "WindowsApps python.exe is an app execution alias, not a usable CPython install."
Write-Host "If cl.exe is only visible under Visual Studio, run from Developer PowerShell or set CMAKE_GENERATOR to Visual Studio 17 2022."
