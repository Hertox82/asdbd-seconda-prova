# Test isolati dei controlli host: non richiedono Docker e non avviano container.
$ErrorActionPreference = 'Stop'
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/common.ps1')

# Simula esclusivamente la ricerca dei container del progetto.
$script:ActiveService = ''
function Invoke-LabDocker {
    param([string[]]$DockerArguments)
    if ($DockerArguments[0] -ne 'ps') { throw 'Chiamata Docker inattesa nel test.' }
    if ($script:ActiveService -and
        ($DockerArguments -contains "label=com.docker.compose.service=$script:ActiveService")) {
        return 'container-sintetico'
    }
}

foreach ($selected in @('postgres', 'mongo', 'cockroach')) {
    $script:ActiveService = ''
    Assert-SingleDatabase -Database $selected
    foreach ($other in @('postgres', 'mongo', 'cockroach')) {
        if ($selected -eq $other) { continue }
        $script:ActiveService = $other
        $refused = $false
        try { Assert-SingleDatabase -Database $selected }
        catch {
            if ($_.Exception.Message -notlike "Avvio rifiutato: $other*") { throw }
            $refused = $true
        }
        if (-not $refused) { throw "$selected avviabile mentre $other e attivo." }
        Write-Output "OK: $selected rifiutato con $other attivo."
    }
}

$firstLock = Open-LabLock
try {
    $refused = $false
    try { $unexpectedLock = Open-LabLock; $unexpectedLock.Dispose() }
    catch { $refused = $true }
    if (-not $refused) { throw 'Due script hanno acquisito lo stesso lock.' }
}
finally { $firstLock.Dispose() }
$newLock = Open-LabLock
$newLock.Dispose()
Write-Output 'OK: esclusione reciproca e rilascio del lock.'

foreach ($name in @('start-db.ps1', 'stop-db.ps1')) {
    $refused = $false
    try { & (Join-Path (Split-Path -Parent $PSScriptRoot) "scripts/$name") -Database redis }
    catch {
        if ($_.Exception -isnot [System.Management.Automation.ParameterBindingException]) { throw }
        $refused = $true
    }
    if (-not $refused) { throw "$name ha accettato un database di un task successivo." }
    Write-Output "OK: $name rifiuta database non implementati."
}
