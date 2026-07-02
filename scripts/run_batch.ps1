param(
    [int]$Count = 3
)

$ErrorActionPreference = "Stop"
$logDir = "docs/logs"
$projectRoot = Resolve-Path "$PSScriptRoot/.."

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

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

function Get-RowCount {
    param([string]$line, [string]$pattern)
    if ($line -match $pattern) { return $matches[1] }
    return $null
}

Set-Location $projectRoot
Log "=== BATCH RUN (${Count}x ETL) ==="
Log "Project root: $projectRoot"
Log ""

# 1. EXTRACT (1x)
$extractLog = "$logDir/batch_extract.log"
Log ">>> EXTRACT -> $extractLog"
"=== EXTRACT $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $extractLog

$t0 = Get-Date
uv run python spark/batch_extract.py 2>&1 | Add-Content $extractLog
$t1 = Get-Date
$durExtract = [math]::Round(($t1 - $t0).TotalSeconds)

$extractLines = Get-Content $extractLog

$rowCount = 0
$extractOk = $false
foreach ($line in $extractLines) {
    if ($line -match 'Batch Extract selesai.*total (\d+) rows') { $rowCount = [int]$matches[1]; $extractOk = $true }
}

if ($extractOk) {
    Invoke-Audit -dagId "manual_extract" -runId "extract_$(Get-Date -Format 'yyyyMMdd_HHmmss')" -status SUCCESS -recordsOut $rowCount
    Log "Extract OK: $rowCount rows (${durExtract}s)"
} else {
    Log "Extract FAILED"
    Invoke-Audit -dagId "manual_extract" -runId "extract_$(Get-Date -Format 'yyyyMMdd_HHmmss')" -status FAILED
}
Log ""

# 2. ETL LOOP (Nx)
$etlResults = @()

for ($i = 1; $i -le $Count; $i++) {
    $etlLog = "$logDir/batch_etl_run_${i}.log"
    $runId = "etl_run_${i}_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    Log ">>> ETL RUN #${i} -> $etlLog"

    "=== BATCH ETL RUN #${i} $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" | Out-File -FilePath $etlLog

    $t0 = Get-Date
    docker compose exec -u root spark-master pip install boto3 botocore psycopg2-binary 2>&1 | Out-Null
    docker compose exec -u root spark-master spark-submit `
        --packages org.postgresql:postgresql:42.7.1 `
        --conf spark.sql.ansi.enabled=false `
        --conf spark.jars.ivy=/tmp/.ivy2 `
        /opt/airflow/spark/batch_etl.py 2>&1 | Add-Content $etlLog
    $t1 = Get-Date

    $durEtl = [math]::Round(($t1 - $t0).TotalSeconds)
    $etlLines = Get-Content $etlLog

    $startTime = ""
    $endTime = ""
    $inserted = 0
    $etlOk = $false

    foreach ($line in $etlLines) {
        if ($line -match 'Batch ETL mulai') { $startTime = Get-Timestamp $line }
        if ($line -match 'Batch ETL selesai') { $endTime = Get-Timestamp $line; $etlOk = $true }
        if ($line -match 'Insert (\d+) baris') { $inserted = [int]$matches[1] }
    }

    $etlResults += @{
        run       = $i
        log       = $etlLog
        start     = $startTime
        end       = $endTime
        dur       = $durEtl
        inserted  = $inserted
        ok        = $etlOk
    }

    if ($etlOk) {
        Invoke-Audit -dagId "manual_etl" -runId $runId -status SUCCESS -recordsIn 43920 -recordsOut $inserted
        Log "ETL #${i} OK: ${inserted} inserted (${durEtl}s)"
    } else {
        Invoke-Audit -dagId "manual_etl" -runId $runId -status FAILED
        Log "ETL #${i} FAILED (${durEtl}s)"
    }
    Log ""
}

# 3. RINGKASAN
Log "============================================"
Log "           RINGKASAN EKSEKUSI BATCH         "
Log "============================================"
Log ""
Log "EXTRACT"
if ($extractOk) {
    Log "  File     : $extractLog"
    Log "  Status   : SUCCESS"
    Log "  Rows     : $rowCount"
    Log "  Durasi   : ${durExtract}s"
} else {
    Log "  File     : $extractLog"
    Log "  Status   : FAILED"
}
Log ""

Log "ETL (${Count}x)"
foreach ($r in $etlResults) {
    $status = if ($r.ok) { "SUCCESS" } else { "FAILED" }
    Log "  Run #${($r.run)}"
    Log "    File   : $($r.log)"
    Log "    Status : $status"
    if ($r.start) { Log "    Start  : $($r.start)" }
    if ($r.end) { Log "    End    : $($r.end)" }
    Log "    Durasi : $($r.dur)s"
    if ($r.ok) { Log "    Insert : $($r.inserted) rows" }
    Log ""
}

Log "Log files:"
Log "  Extract : $extractLog"
foreach ($r in $etlResults) {
    Log "  ETL #$($r.run) : $($r.log)"
}

# Final DB check
Log ""
Log "Verifikasi PostgreSQL:"
docker compose exec -u root postgres psql -U aqi_user -d aqi_db -c "SELECT dag_id, status, records_in, records_out, started_at FROM pipeline_audit ORDER BY id;" 2>&1
docker compose exec -u root postgres psql -U aqi_user -d aqi_db -c "SELECT count(*) AS total_rows, min(date) AS earliest, max(date) AS latest FROM daily_aqi;" 2>&1

Log ""
Log "=== BATCH RUN SELESAI ==="
