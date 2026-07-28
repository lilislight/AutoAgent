$ErrorActionPreference = "Stop"

$RepositoryRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonCommand = $null
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
    Write-Error "Python 3 is required to build AutoAgent."
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Error "npm is required to build the tracing UI."
}

& $PythonCommand @PythonPrefix -c "import build"
if ($LASTEXITCODE -ne 0) {
    Write-Error (
        "Python package 'build' is required. Install it with: " +
        "$PythonCommand -m pip install build"
    )
}

$BuildScript = Join-Path $RepositoryRoot "scripts\build_wheel.py"
& $PythonCommand @PythonPrefix $BuildScript
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
