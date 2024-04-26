Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

# Find vcpkg or download it if required
if ($null -ne $Env:VCPKG_INSTALLATION_ROOT) {
  $vcpkgDir = $Env:VCPKG_INSTALLATION_ROOT
  Write-Output "Using vcpkg at $vcpkgDir from VCPKG_INSTALLATION_ROOT"
}
elseif ($null -ne $Env:VCPKG_ROOT) {
  $vcpkgDir = $Env:VCPKG_ROOT
  Write-Output "Using vcpkg at $vcpkgDir from VCPKG_ROOT"
}
else {
  $vcpkgDir = "$(Get-Location)/build/vcpkg"
  Write-Output "Using local vcpkg at $vcpkgDir"
  if (-not (Test-Path $vcpkgDir)) {
    git clone https://github.com/microsoft/vcpkg.git $vcpkgDir
    if (-not $?) { throw "git clone failed" }
    & $vcpkgDir/bootstrap-vcpkg.bat
    if (-not $?) { throw "bootstrap-vcpkg failed" }
  }
}

$triplet = "arm64-windows-static-clang"

$build_types = @("Release")

$options = @()

$customTripletsDir = "$(Get-Location)/custom-triplets"
$options += "-D"
$options += "VCPKG_OVERLAY_TRIPLETS=$customTripletsDir"


# Note: -A is the host platform, keep as x64 for cross compiling to ARM
cmake -B build/$triplet -S . -D VCPKG_TARGET_TRIPLET=$triplet -D CMAKE_TOOLCHAIN_FILE=$vcpkgDir/scripts/buildsystems/vcpkg.cmake -G "Visual Studio 17 2022" -A "x64" $options -D VCPKG_INSTALL_OPTIONS="--allow-unsupported"
if (-not $?) { throw "cmake failed" }

foreach ($build_type in $build_types) {
  msbuild build/$triplet/ParquetSharp.sln -t:ParquetSharpNative:Rebuild -p:Configuration=$build_type
  if (-not $?) { throw "msbuild failed" }
}
