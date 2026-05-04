# ============================================================
# 🚀 SCRIPT: Start ALL Nodes + Health Check + Locust Load Test
# ============================================================
# Jalankan di PowerShell:
#     powershell -ExecutionPolicy Bypass -File run_all_local.ps1
#
# CATATAN: Script ini menjalankan 9 node (3 Lock, 3 Queue, 3 Cache)
#          sebagai background process, lalu health check, lalu Locust.
# ============================================================

$ErrorActionPreference = "Continue"
$ROOT = $PSScriptRoot
Set-Location $ROOT

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  DISTRIBUTED SYNC SYSTEM - Full Local Startup" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# --- Kill old processes on our ports ---
Write-Host "[0/6] Membersihkan proses lama..." -ForegroundColor Yellow
$ports = @(7001,7002,7003,7101,7102,7103,7201,7202,7203,8089)
foreach ($port in $ports) {
    $conns = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
    foreach ($conn in $conns) {
        Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}
Start-Sleep -Seconds 2
Write-Host "  [OK] Port dibersihkan" -ForegroundColor Green
Write-Host ""

# --- PEERS JSON ---
$LOCK_PEERS  = '[{"id":"lock-1","url":"http://localhost:7001"},{"id":"lock-2","url":"http://localhost:7002"},{"id":"lock-3","url":"http://localhost:7003"}]'
$QUEUE_PEERS = '[{"id":"queue-1","url":"http://localhost:7101"},{"id":"queue-2","url":"http://localhost:7102"},{"id":"queue-3","url":"http://localhost:7103"}]'
$CACHE_PEERS = '[{"id":"cache-1","url":"http://localhost:7201"},{"id":"cache-2","url":"http://localhost:7202"},{"id":"cache-3","url":"http://localhost:7203"}]'

# ============================================================
# STEP 1: Start Lock Manager Nodes (port 7001, 7002, 7003)
# ============================================================
Write-Host "[1/6] Starting Lock Manager nodes..." -ForegroundColor Cyan

$lockConfigs = @(
    @{NODE_ID="lock-1"; PORT="7001"},
    @{NODE_ID="lock-2"; PORT="7002"},
    @{NODE_ID="lock-3"; PORT="7003"}
)

$processes = @()
foreach ($cfg in $lockConfigs) {
    $env:NODE_ID = $cfg.NODE_ID
    $env:NODE_ROLE = "lock_manager"
    $env:HOST = "0.0.0.0"
    $env:PORT = $cfg.PORT
    $env:PEERS = $LOCK_PEERS
    $env:LOG_LEVEL = "WARNING"
    $env:RAFT_ELECTION_TIMEOUT_MS = "1500"
    $env:RAFT_HEARTBEAT_MS = "500"
    $env:LOCK_REQUEST_TIMEOUT_MS = "5000"

    $proc = Start-Process -FilePath "python" -ArgumentList "-m", "src.main" -WorkingDirectory $ROOT -PassThru -WindowStyle Hidden
    $processes += $proc
    Write-Host "  [OK] $($cfg.NODE_ID) started (PID: $($proc.Id)) on port $($cfg.PORT)" -ForegroundColor Green
}

# ============================================================
# STEP 2: Start Queue Nodes (port 7101, 7102, 7103)
# ============================================================
Write-Host ""
Write-Host "[2/6] Starting Queue nodes..." -ForegroundColor Cyan

$queueConfigs = @(
    @{NODE_ID="queue-1"; PORT="7101"},
    @{NODE_ID="queue-2"; PORT="7102"},
    @{NODE_ID="queue-3"; PORT="7103"}
)

foreach ($cfg in $queueConfigs) {
    $env:NODE_ID = $cfg.NODE_ID
    $env:NODE_ROLE = "queue_node"
    $env:HOST = "0.0.0.0"
    $env:PORT = $cfg.PORT
    $env:PEERS = $QUEUE_PEERS
    $env:LOG_LEVEL = "WARNING"

    $proc = Start-Process -FilePath "python" -ArgumentList "-m", "src.main" -WorkingDirectory $ROOT -PassThru -WindowStyle Hidden
    $processes += $proc
    Write-Host "  [OK] $($cfg.NODE_ID) started (PID: $($proc.Id)) on port $($cfg.PORT)" -ForegroundColor Green
}

# ============================================================
# STEP 3: Start Cache Nodes (port 7201, 7202, 7203)
# ============================================================
Write-Host ""
Write-Host "[3/6] Starting Cache nodes..." -ForegroundColor Cyan

$cacheConfigs = @(
    @{NODE_ID="cache-1"; PORT="7201"},
    @{NODE_ID="cache-2"; PORT="7202"},
    @{NODE_ID="cache-3"; PORT="7203"}
)

foreach ($cfg in $cacheConfigs) {
    $env:NODE_ID = $cfg.NODE_ID
    $env:NODE_ROLE = "cache_node"
    $env:HOST = "0.0.0.0"
    $env:PORT = $cfg.PORT
    $env:PEERS = $CACHE_PEERS
    $env:LOG_LEVEL = "WARNING"

    $proc = Start-Process -FilePath "python" -ArgumentList "-m", "src.main" -WorkingDirectory $ROOT -PassThru -WindowStyle Hidden
    $processes += $proc
    Write-Host "  [OK] $($cfg.NODE_ID) started (PID: $($proc.Id)) on port $($cfg.PORT)" -ForegroundColor Green
}

# ============================================================
# STEP 4: Wait for nodes to start + Raft election
# ============================================================
Write-Host ""
Write-Host "[4/6] Menunggu nodes ready + Raft election (8 detik)..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

# ============================================================
# STEP 5: Health Check semua node
# ============================================================
Write-Host ""
Write-Host "[5/6] Health Check semua node..." -ForegroundColor Cyan

$allUrls = @(
    @{Name="Lock-1";  Url="http://localhost:7001/health"},
    @{Name="Lock-2";  Url="http://localhost:7002/health"},
    @{Name="Lock-3";  Url="http://localhost:7003/health"},
    @{Name="Queue-1"; Url="http://localhost:7101/health"},
    @{Name="Queue-2"; Url="http://localhost:7102/health"},
    @{Name="Queue-3"; Url="http://localhost:7103/health"},
    @{Name="Cache-1"; Url="http://localhost:7201/health"},
    @{Name="Cache-2"; Url="http://localhost:7202/health"},
    @{Name="Cache-3"; Url="http://localhost:7203/health"}
)

$allHealthy = $true
foreach ($item in $allUrls) {
    try {
        $resp = Invoke-RestMethod -Uri $item.Url -Method Get -TimeoutSec 5
        Write-Host "  [OK] $($item.Name) => status: $($resp.status), node_id: $($resp.node_id)" -ForegroundColor Green
    } catch {
        Write-Host "  [FAIL] $($item.Name) => NOT REACHABLE" -ForegroundColor Red
        $allHealthy = $false
    }
}

# Check Raft leader
Write-Host ""
Write-Host "  Checking Raft leader..." -ForegroundColor Yellow
try {
    $leader = Invoke-RestMethod -Uri "http://localhost:7001/lock/leader" -Method Get -TimeoutSec 5
    Write-Host "  [OK] Raft Leader: $($leader.leader_id)" -ForegroundColor Green
} catch {
    Write-Host "  [WARN] Leader belum terpilih, akan terpilih saat test berjalan" -ForegroundColor Yellow
}

if (-not $allHealthy) {
    Write-Host ""
    Write-Host "  [ERROR] Beberapa node tidak bisa diakses!" -ForegroundColor Red
    Write-Host "  Pastikan tidak ada error di log node." -ForegroundColor Red
    Write-Host ""
}

# ============================================================
# STEP 6: Info cara jalankan test
# ============================================================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  SEMUA NODE BERJALAN!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Lock Managers : http://localhost:7001, 7002, 7003" -ForegroundColor White
Write-Host "  Queue Nodes   : http://localhost:7101, 7102, 7103" -ForegroundColor White
Write-Host "  Cache Nodes   : http://localhost:7201, 7202, 7203" -ForegroundColor White
Write-Host ""
Write-Host "  Swagger UI    : http://localhost:7001/swagger" -ForegroundColor White
Write-Host ""
Write-Host "------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "  JALANKAN TEST (di terminal baru):" -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. Demo Client (functional test):" -ForegroundColor White
Write-Host "     python src/demo_client.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "  2. Locust Load Test (Lock only):" -ForegroundColor White
Write-Host "     locust -f benchmarks/load_test_scenarios.py LockUser" -ForegroundColor Cyan
Write-Host "     -> Buka http://localhost:8089" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Locust Load Test (semua komponen):" -ForegroundColor White
Write-Host "     locust -f benchmarks/load_test_scenarios.py" -ForegroundColor Cyan
Write-Host "     -> Buka http://localhost:8089" -ForegroundColor Gray
Write-Host ""
Write-Host "  4. Pytest (unit/integration test):" -ForegroundColor White
Write-Host "     python -m pytest tests/ -v" -ForegroundColor Cyan
Write-Host "------------------------------------------------------------" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Tekan Ctrl+C untuk STOP semua nodes." -ForegroundColor Red
Write-Host ""

# Wait for user to stop
try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    Write-Host ""
    Write-Host "Stopping all nodes..." -ForegroundColor Yellow
    foreach ($proc in $processes) {
        if (-not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            Write-Host "  Stopped PID $($proc.Id)" -ForegroundColor Gray
        }
    }
    Write-Host "  [OK] All nodes stopped." -ForegroundColor Green
}
