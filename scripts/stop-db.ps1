<#
.SYNOPSIS
Arresta il DB selezionato conservando container e volumi; Jupyter resta attivo.
.PARAMETER Database
Sono supportati postgres, mongo e cockroach.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('postgres', 'mongo', 'cockroach')][string]$Database
)

. (Join-Path $PSScriptRoot 'common.ps1')
$Database = $Database.ToLowerInvariant()
$lock = Open-LabLock
try {
    $null = Assert-LabPrerequisites
    Invoke-LabCompose -ComposeArguments @('--profile', $Database, 'stop', '--timeout', '60', $Database)
    Write-Host "$Database arrestato; dati e volumi conservati. Jupyter resta disponibile."
}
finally { $lock.Dispose() }
