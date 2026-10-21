# test-workloads/simulate-alert-storm.ps1
# Dispatches 10 distinct alerts to simulate a cluster-wide outage and trigger KEDA autoscaling

$uri = "http://localhost/api/v1/alerts"
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  KubeHealer: Triggering Alert Storm (10 Incidents)        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

1..10 | ForEach-Object {
    $id = $_
    $body = @{
        receiver = "kubehealer-gateway-receiver"
        status   = "firing"
        alerts   = @(
            @{
                status = "firing"
                labels = @{
                    alertname  = "PodOOMKilled"
                    severity   = "critical"
                    namespace  = "sandbox-apps"
                    pod        = "oom-service-storm-$id"
                    deployment = "oom-service"
                }
                annotations = @{
                    summary = "Pod OOMKilled incident $id during alert storm"
                }
                startsAt    = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
                fingerprint = "storm-fp-$id-$(Get-Random)"
            }
        )
    } | ConvertTo-Json -Depth 5

    try {
        Invoke-RestMethod -Uri $uri -Method Post -Body $body -ContentType "application/json" | Out-Null
        Write-Host "  [+] Dispatched Alert $id / 10 into alert-gateway" -ForegroundColor Green
    } catch {
        Write-Host "  [-] Failed to dispatch alert $($id): $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "==========================================================" -ForegroundColor Yellow
Write-Host "  All 10 alerts published into Kafka topic!               " -ForegroundColor Yellow
Write-Host "  Watch KEDA scale agent-worker from 1 to 5 pods!         " -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Yellow
