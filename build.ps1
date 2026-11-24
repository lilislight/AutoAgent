$ErrorActionPreference = "Stop"

$RepositoryRoot = $PSScriptRoot
$PythonPrefix = @()
if ($env:PYTHON) {
    $PythonCommand = $env:PYTHON
}
elseif (Test-Path (Join-Path $RepositoryRoot ".venv\Scripts\python.exe")) {
    $PythonCommand = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
}
elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $PythonCommand = (Get-Command python).Source
}
elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $PythonCommand = (Get-Command py).Source
    $PythonPrefix = @("-3")
}
else {
    throw "Python 3 is required to build AutoAgent."
}

$NpmCommand = Get-Command npm.cmd -ErrorAction SilentlyContinue
if (-not $NpmCommand) {
    $NpmCommand = Get-Command npm -ErrorAction SilentlyContinue
}
if (-not $NpmCommand) {
    throw "Node.js and npm are required to build the web UI."
}
& $PythonCommand @PythonPrefix -c "import build, setuptools, wheel"
if ($LASTEXITCODE -ne 0) {
    throw "Install requirements-dev.txt in the selected Python environment first."
}

$Staging = Join-Path ([System.IO.Path]::GetTempPath()) ("autoagent-wheel-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $Staging | Out-Null
try {
    Copy-Item (Join-Path $RepositoryRoot "autoagent") (Join-Path $Staging "autoagent") -Recurse
    $Static = Join-Path $Staging "autoagent/server/static"
    if (Test-Path $Static) {
        Remove-Item $Static -Recurse -Force
    }
    Get-ChildItem (Join-Path $Staging "autoagent") -Directory -Filter "__pycache__" -Recurse |
        Remove-Item -Recurse -Force
    Copy-Item (Join-Path $RepositoryRoot "pyproject.toml") $Staging
    Get-ChildItem $RepositoryRoot -File -Filter "LICENSE*" | Copy-Item -Destination $Staging

    Write-Host "Building web UI..."
    Push-Location (Join-Path $RepositoryRoot "ui")
    try {
        & $NpmCommand.Source ci
        if ($LASTEXITCODE -ne 0) { throw "Installing web UI dependencies failed." }
        & $NpmCommand.Source run build -- --outDir $Static
        if ($LASTEXITCODE -ne 0) { throw "Building the web UI failed." }
    }
    finally {
        Pop-Location
    }

    Write-Host "Building wheel..."
    $Output = Join-Path $RepositoryRoot "dist"
    & $PythonCommand @PythonPrefix -m build --wheel --no-isolation --outdir $Output $Staging
    if ($LASTEXITCODE -ne 0) { throw "Building the wheel failed." }
    Write-Host "Wheel saved to $Output"
}
finally {
    Remove-Item $Staging -Recurse -Force
}
