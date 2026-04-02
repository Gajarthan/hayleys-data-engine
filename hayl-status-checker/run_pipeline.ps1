$ErrorActionPreference = "Stop"

$projectRoot = "C:\Users\gajar\PycharmProjects\PythonProject4"
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$logDir = Join-Path $projectRoot "hayl-status-checker\data\logs"
$logFile = Join-Path $logDir "pipeline.log"

if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

Set-Location -LiteralPath $projectRoot

function Write-Log {
    param([string]$Message)
    "[$(Get-Date -Format o)] $Message" | Out-File -FilePath $logFile -Append -Encoding utf8
}

function Invoke-PipelineStep {
    param(
        [string]$ScriptRelativePath
    )

    $scriptPath = Join-Path $projectRoot $ScriptRelativePath
    if (-not (Test-Path -LiteralPath $scriptPath)) {
        throw "Script not found: $scriptPath"
    }

    $stdoutTemp = [System.IO.Path]::GetTempFileName()
    $stderrTemp = [System.IO.Path]::GetTempFileName()

    try {
        Write-Log "Running $ScriptRelativePath"
        $process = Start-Process -FilePath $pythonExe -ArgumentList $scriptPath -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdoutTemp -RedirectStandardError $stderrTemp

        $stdoutLines = Get-Content -Path $stdoutTemp
        if ($stdoutLines) {
            $stdoutLines | Out-File -FilePath $logFile -Append -Encoding utf8
        }

        $stderrLines = Get-Content -Path $stderrTemp
        if ($stderrLines) {
            $stderrLines | Out-File -FilePath $logFile -Append -Encoding utf8
        }

        if ($process.ExitCode -ne 0) {
            throw "Step failed ($ScriptRelativePath) with exit code $($process.ExitCode)"
        }

        Write-Log "Completed $ScriptRelativePath"
    }
    finally {
        Remove-Item -LiteralPath $stdoutTemp, $stderrTemp -ErrorAction SilentlyContinue
    }
}

try {
    Write-Log "Starting pipeline"
    Invoke-PipelineStep "hayl-status-checker\fetch_hayl_stock.py"
    Invoke-PipelineStep "hayl-status-checker\scrape_hayleys_reports.py"
    Invoke-PipelineStep "hayl-status-checker\extract_reports_with_opencode.py"
    Write-Log "Pipeline completed"
}
catch {
    Write-Log "Pipeline failed: $($_.Exception.Message)"
    throw
}
