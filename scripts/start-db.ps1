<#
.SYNOPSIS
Avvia Jupyter e un solo database del laboratorio, senza cancellare dati.
.DESCRIPTION
Policy: rifiuta l'avvio se un altro DB del progetto e attivo. Attende gli
healthcheck con timeout; non avvia dipendenze implicite o servizi di altri profili.
.PARAMETER Database
Sono supportati postgres, mongo e cockroach in esecuzione sequenziale.
.PARAMETER TimeoutSeconds
Tempo massimo di attesa degli healthcheck, escluso il download/build iniziale.
.PARAMETER Build
Ricostruisce Jupyter dopo modifiche a Dockerfile o requirements. Il primo avvio
costruisce comunque l'immagine se non esiste; i riavvii ordinari la riutilizzano.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('postgres', 'mongo', 'cockroach')][string]$Database,
    [ValidateRange(15, 600)][int]$TimeoutSeconds = 120,
    [switch]$Build
)

. (Join-Path $PSScriptRoot 'common.ps1')
$Database = $Database.ToLowerInvariant()
$lock = Open-LabLock
try {
    $engine = Assert-LabPrerequisites
    Assert-SingleDatabase -Database $Database
    # Il JSON contiene segreti: conservarlo in memoria, non stamparlo o salvarlo.
    $config = (Invoke-LabCompose -ComposeArguments @('--profile', $Database,
        'config', '--format', 'json')) | ConvertFrom-Json
    $budget = [long]$config.services.jupyter.mem_limit + [long]$config.services.$Database.mem_limit
    $margin = 512MB
    Write-Host ('Docker: {0:N2} GiB RAM, {1} CPU; limiti Jupyter + DB: {2:N2} GiB.' -f
        ($engine.MemTotal / 1GB), $engine.NCPU, ($budget / 1GB))
    if ($budget + $margin -gt $engine.MemTotal) {
        throw 'Budget RAM eccessivo: lasciare almeno 512 MiB al motore. Rivedere .env e risorse WSL2.'
    }
    $localImage = @(Invoke-LabDocker -DockerArguments @('image', 'ls', '--quiet', 'asdbd-jupyter:task1'))
    $upArguments = @('--profile', $Database, 'up', '--detach', '--no-deps',
        '--wait', '--wait-timeout', "$TimeoutSeconds")
    if ($Build -or $localImage.Count -eq 0) { $upArguments += '--build' }
    Write-Host "Avvio Jupyter e $Database; il primo pull/build richiede rete e alcuni minuti."
    try {
        Invoke-LabCompose -ComposeArguments ($upArguments + @('jupyter', $Database))
    }
    catch {
        Write-Warning "Avvio incompleto. I volumi sono conservati. Controllare docker compose ps -a e docker compose logs --tail 40 $Database."
        throw
    }
    Assert-SingleDatabase -Database $Database
    $port = $config.services.jupyter.ports[0].published
    Write-Host "Servizi pronti. Jupyter: http://127.0.0.1:$port/lab"
    Write-Host 'Inserire nel login il JUPYTER_TOKEN del proprio .env (non viene stampato).'
    Write-Host 'DEMO sui dati persistenti: notebooks/05_demo.ipynb. Storici e grafici: notebooks/06_results_analysis.ipynb.'
}
finally { $lock.Dispose() }
