param(
    [int]$TrainCount = 1,
    [switch]$SkipTrain,
    [switch]$SkipPredict,
    [switch]$SkipValidation,
    [switch]$SkipTelegram
)

$ErrorActionPreference = "Stop"
$logDir = "docs/logs"
$projectRoot = Resolve-Path "$PSScriptRoot/.."

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

$env:PYTHONIOENCODING = "utf-8"

function Log { param([string]$msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $msg" }

function Invoke-Audit {
    param([string]$dagId, [string]$runId, [string]$status, [int]$recordsIn = 0, [int]$recordsOut = 0)
    $sql = "INSERT INTO pipeline_audit (dag_id, run_id, status, records_in, records_out, started_at, finished_at) VALUES ('$dagId', '$runId', '$status', $recordsIn, $recordsOut, NOW(), NOW());"
    docker compose exec -u root postgres psql -U aqi_user -d aqi_db -c "$sql" 2>&1 | Out-Null
    Log "Audit $dagId/$runId -> $status"
}

function Get-Timestamp {
    param([string]$line)
    if ($line -match '^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})') { return $matches[1] }
    return ""
}

Set-Location $projectRoot
Log "============================================"
Log "           ML PIPELINE RUN                  "
Log "============================================"
Log "Train: $(if ($SkipTrain) { 'SKIP' } else { "${TrainCount}x" })"
Log "Predict: $(if ($SkipPredict) { 'SKIP' } else { '1x' })"
Log "Validation: $(if ($SkipValidation) { 'SKIP' } else { '1x' })"
Log ""

# 1. TRAIN (Nx)
$trainResults = @()

if (-not $SkipTrain) {
    for ($i = 1; $i -le $TrainCount; $i++) {
        $trainLog = "$logDir/ml_train_${i}.log"
        $runId = "train_${i}_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
        Log ">>> TRAIN #${i} -> $trainLog"

        "=== ML TRAIN #${i} $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $trainLog

        $t0 = Get-Date
        uv run python ml/train.py 2>&1 | Add-Content $trainLog
        $t1 = Get-Date

        $dur = [math]::Round(($t1 - $t0).TotalSeconds)
        $lines = Get-Content $trainLog

        $startTime = ""; $endTime = ""
        $loadedRows = 0; $accuracy = ""; $f1 = ""
        $trainOk = $false
        $trainSamples = 0

        foreach ($line in $lines) {
            if ($line -match 'ML Training mulai') { $startTime = Get-Timestamp $line }
            if ($line -match 'ML Training selesai') { $endTime = Get-Timestamp $line; $trainOk = $true }
            if ($line -match 'Loaded (\d+) rows') { $loadedRows = [int]$matches[1] }
            if ($line -match 'RF accuracy: ([\d.]+), f1: ([\d.]+)') { $accuracy = $matches[1]; $f1 = $matches[2] }
            if ($line -match 'Train: (\d+)') { $trainSamples = [int]$matches[1] }
        }

        $trainResults += @{
            run       = $i
            log       = $trainLog
            start     = $startTime
            end       = $endTime
            dur       = $dur
            ok        = $trainOk
            loaded    = $loadedRows
            accuracy  = $accuracy
            f1        = $f1
            samples   = $trainSamples
        }

        if ($trainOk) {
            Invoke-Audit -dagId "manual_ml_train" -runId $runId -status SUCCESS -recordsIn $loadedRows -recordsOut $trainSamples
            Log "TRAIN #${i} OK: ${loadedRows} loaded, ${trainSamples} train samples, acc=$accuracy f1=$f1 (${dur}s)"
            if (-not $SkipTelegram) {
                $body = "$loadedRows loaded, $trainSamples samples, acc=$accuracy f1=$f1 | ${dur}s"
                uv run python scripts/telegram_alert.py --notif "Train #${i}" --status SUCCESS --body "$body" 2>&1 | Out-Null
            }
        } else {
            Invoke-Audit -dagId "manual_ml_train" -runId $runId -status FAILED
            Log "TRAIN #${i} FAILED (${dur}s)"
            if (-not $SkipTelegram) {
                $body = "${dur}s"
                uv run python scripts/telegram_alert.py --notif "Train #${i}" --status FAILED --body "$body" 2>&1 | Out-Null
            }
        }
        Log ""
    }
} else {
    Log ">>> TRAIN: SKIP"
    Log ""
}

# 2. PREDICT (1x)
$predictResult = $null

if (-not $SkipPredict) {
    $predictLog = "$logDir/ml_predict.log"
    $runId = "predict_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    Log ">>> PREDICT -> $predictLog"

    "=== ML PREDICT $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $predictLog

    $t0 = Get-Date
    uv run python ml/predict.py 2>&1 | Add-Content $predictLog
    $t1 = Get-Date

    $dur = [math]::Round(($t1 - $t0).TotalSeconds)
    $lines = Get-Content $predictLog

    $startTime = ""; $endTime = ""
    $loadedRows = 0; $predCount = 0
    $predictOk = $false

    foreach ($line in $lines) {
        if ($line -match 'Batch Predict mulai') { $startTime = Get-Timestamp $line }
        if ($line -match 'Batch Predict selesai') { $endTime = Get-Timestamp $line; $predictOk = $true }
        if ($line -match 'Loaded (\d+) rows') { $loadedRows = [int]$matches[1] }
        if ($line -match 'Disimpan (\d+) prediksi') { $predCount = [int]$matches[1] }
    }

    $predictResult = @{
        log       = $predictLog
        start     = $startTime
        end       = $endTime
        dur       = $dur
        ok        = $predictOk
        loaded    = $loadedRows
        predCount = $predCount
    }

    if ($predictOk) {
        Invoke-Audit -dagId "manual_ml_predict" -runId $runId -status SUCCESS -recordsIn $loadedRows -recordsOut $predCount
        Log "PREDICT OK: ${predCount} predictions (${dur}s)"
        if (-not $SkipTelegram) {
            $body = "$loadedRows rows -> $predCount predictions | ${dur}s"
            uv run python scripts/telegram_alert.py --notif "Predict" --status SUCCESS --body "$body" 2>&1 | Out-Null
        }
    } else {
        Invoke-Audit -dagId "manual_ml_predict" -runId $runId -status FAILED
        Log "PREDICT FAILED (${dur}s)"
        if (-not $SkipTelegram) {
            $body = "${dur}s"
            uv run python scripts/telegram_alert.py --notif "Predict" --status FAILED --body "$body" 2>&1 | Out-Null
        }
    }
    Log ""
} else {
    Log ">>> PREDICT: SKIP"
    Log ""
}

# 3. VALIDATION (1x)
$validationResult = $null

if (-not $SkipValidation) {
    $validateLog = "$logDir/ml_validation.log"
    $runId = "validate_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    Log ">>> VALIDATION -> $validateLog"

    "=== ML VALIDATION $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $validateLog

    $t0 = Get-Date
    uv run python ml/validation.py 2>&1 | Add-Content $validateLog
    $t1 = Get-Date

    $dur = [math]::Round(($t1 - $t0).TotalSeconds)
    $lines = Get-Content $validateLog

    $startTime = ""; $endTime = ""
    $validateOk = $false
    $warnings = @()

    foreach ($line in $lines) {
        if ($line -match 'Data Quality Check') { $startTime = Get-Timestamp $line }
        if ($line -match 'Quality Check selesai') { $endTime = Get-Timestamp $line; $validateOk = $true }
        if ($line -match 'WARNING') { $warnings += $line }
    }

    $validationResult = @{
        log       = $validateLog
        start     = $startTime
        end       = $endTime
        dur       = $dur
        ok        = $validateOk
        warnings  = $warnings
        nWarnings = $warnings.Count
    }

    if ($validateOk) {
        Invoke-Audit -dagId "manual_ml_validation" -runId $runId -status SUCCESS
        Log "VALIDATION OK: $($validationResult.nWarnings) warnings (${dur}s)"
        if ($validationResult.nWarnings -gt 0) {
            foreach ($w in $warnings) { Log "  WARN: $w" }
        }
        if (-not $SkipTelegram) {
            $body = "$($validationResult.nWarnings) warnings | ${dur}s"
            uv run python scripts/telegram_alert.py --notif "Validation" --status SUCCESS --body "$body" 2>&1 | Out-Null
        }
    } else {
        Invoke-Audit -dagId "manual_ml_validation" -runId $runId -status FAILED
        Log "VALIDATION FAILED (${dur}s)"
        if (-not $SkipTelegram) {
            $body = "${dur}s"
            uv run python scripts/telegram_alert.py --notif "Validation" --status FAILED --body "$body" 2>&1 | Out-Null
        }
    }
    Log ""
} else {
    Log ">>> VALIDATION: SKIP"
    Log ""
}

# 4. RINGKASAN
Log "============================================"
Log "           RINGKASAN ML PIPELINE            "
Log "============================================"
Log ""

if (-not $SkipTrain) {
    Log "TRAIN (${TrainCount}x)"
    foreach ($r in $trainResults) {
        $status = if ($r.ok) { "SUCCESS" } else { "FAILED" }
        Log "  Run #$($r.run): $status | $($r.loaded) rows, $($r.samples) train samples, acc=$($r.accuracy) f1=$($r.f1) | $($r.dur)s"
    }
    Log ""
}

if (-not $SkipPredict -and $predictResult) {
    $status = if ($predictResult.ok) { "SUCCESS" } else { "FAILED" }
    Log "PREDICT: $status | $($predictResult.predCount) predictions from $($predictResult.loaded) rows | $($predictResult.dur)s"
    Log ""
}

if (-not $SkipValidation -and $validationResult) {
    $status = if ($validationResult.ok) { "SUCCESS" } else { "FAILED" }
    Log "VALIDATION: $status | $($validationResult.nWarnings) warnings | $($validationResult.dur)s"
    Log ""
}

Log "Log files:"
if (-not $SkipTrain) { foreach ($r in $trainResults) { Log "  Train #$($r.run): $($r.log)" } }
if (-not $SkipPredict -and $predictResult) { Log "  Predict: $($predictResult.log)" }
if (-not $SkipValidation -and $validationResult) { Log "  Validation: $($validationResult.log)" }

# Final DB check
Log ""
Log "Verifikasi PostgreSQL:"
docker compose exec -u root postgres psql -U aqi_user -d aqi_db -c "SELECT dag_id, status, records_in, records_out FROM pipeline_audit WHERE dag_id LIKE 'manual_ml%' AND started_at >= NOW() - interval '5 minutes' ORDER BY id;" 2>&1
docker compose exec -u root postgres psql -U aqi_user -d aqi_db -c "SELECT count(*) AS predictions_total FROM predictions;" 2>&1

if (-not $SkipTelegram) {
    $telegramLog = "$logDir/ml_telegram.log"
    Log ">>> TELEGRAM -> $telegramLog"
    "=== ML TELEGRAM $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $telegramLog
    uv run python scripts/telegram_alert.py 2>&1 | Add-Content $telegramLog
    Log ""
}

Log ""
Log "=== ML PIPELINE SELESAI ==="
