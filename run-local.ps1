# Windows: right-click -> Run with PowerShell, or:  powershell -ExecutionPolicy Bypass -File run-local.ps1
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Url = "http://localhost:8080"
$Admin = if ($env:ADMIN_TOKEN) { $env:ADMIN_TOKEN } else { "dev-admin-token" }
$env:ADMIN_TOKEN = $Admin

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Write-Host "Install Docker Desktop first: https://www.docker.com/products/docker-desktop/"; exit 1 }
Write-Host "==> building and starting (first run downloads images, ~2-4 min)"
docker compose up --build -d
if ($LASTEXITCODE -ne 0) { Write-Host "docker compose failed - is Docker Desktop running?"; exit 1 }

Write-Host -NoNewline "==> waiting for $Url/readyz "
$ready = $false
for ($i = 0; $i -lt 120; $i++) {
  try { if ((Invoke-WebRequest "$Url/readyz" -UseBasicParsing -TimeoutSec 2).StatusCode -eq 200) { $ready = $true; break } } catch {}
  Write-Host -NoNewline "."; Start-Sleep 2
}
if (-not $ready) { Write-Host "`nnot ready - logs:"; docker compose logs --tail 50 app; exit 1 }
Write-Host " ready"

function Post($path, $body, $token) {
  $h = @{}; if ($token) { $h["Authorization"] = "Bearer $token" }
  try { $r = Invoke-WebRequest "$Url$path" -Method Post -Body $body -Headers $h -UseBasicParsing; return @{ code = [int]$r.StatusCode; body = ($r.Content | ConvertFrom-Json) } }
  catch { return @{ code = [int]$_.Exception.Response.StatusCode; body = $null } }
}
Write-Host "==> smoke test"
$show = (Post "/shows" '{"name":"friday-night","seats":["A1","A2","A12","A13"],"price_paise":25000}' $Admin).body.id
$ta = (Post "/auth/token" '{"user_id":"alice"}').body.token
$tb = (Post "/auth/token" '{"user_id":"bob"}').body.token
$a = (Post "/shows/$show/reserve" '{"seats":["A12"],"idempotency_key":"smoke-1"}' $ta).code
$r = (Post "/shows/$show/reserve" '{"seats":["A12"],"idempotency_key":"smoke-1"}' $ta).code
$b = (Post "/shows/$show/reserve" '{"seats":["A12"],"idempotency_key":"smoke-2"}' $tb).code
Write-Host "   alice reserves A12 -> $a (want 201)"
Write-Host "   alice retries same key -> $r (want 200 replay)"
Write-Host "   bob wants A12 -> $b (want 409 seat_taken)"
if ("$a$r$b" -eq "201200409") { Write-Host "   smoke test PASSED" } else { Write-Host "   smoke test FAILED" }

Write-Host "`nRunning at $Url   (show: $Url/shows/$show   metrics: $Url/metrics)"
Write-Host "Burst test: docker run --rm --network host -e ADMIN_TOKEN -v ${PWD}\burst:/burst eclipse-temurin:21-jdk java /burst/Burst.java -url http://host.docker.internal:8080"
Write-Host "Logs: docker compose logs -f app      Stop: docker compose down"
Start-Process "$Url/shows/$show"
