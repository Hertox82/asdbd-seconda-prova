# Funzioni condivise: nessun parsing manuale di .env, lo interpreta Compose.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:RepositoryRoot = Split-Path -Parent $PSScriptRoot
$script:ProjectName = 'asdbd-seconda-prova'

function Invoke-LabDocker {
    param([Parameter(Mandatory = $true)][string[]]$DockerArguments)
    & docker @DockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker ha restituito il codice $LASTEXITCODE. Controllare Docker Desktop e la configurazione."
    }
}

function Invoke-LabCompose {
    param([Parameter(Mandatory = $true)][string[]]$ComposeArguments)
    $arguments = @('compose', '--project-name', $script:ProjectName,
        '--project-directory', $script:RepositoryRoot,
        '--env-file', (Join-Path $script:RepositoryRoot '.env'),
        '--file', (Join-Path $script:RepositoryRoot 'docker-compose.yml'))
    Invoke-LabDocker -DockerArguments ($arguments + $ComposeArguments)
}

function Open-LabLock {
    # Il lock viene rilasciato anche in caso di errore; evita due start simultanei.
    try {
        return [System.IO.File]::Open((Join-Path $script:RepositoryRoot '.lab-infra.lock'),
            [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None)
    }
    catch { throw 'Un altro script infrastrutturale e gia in esecuzione. Riprovare al termine.' }
}

function Assert-LabPrerequisites {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker non trovato nel PATH. Avviare Docker Desktop e riaprire PowerShell.'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $script:RepositoryRoot '.env'))) {
        throw 'Manca .env: copiare .env.example e compilare password e token locali.'
    }
    $engine = (Invoke-LabDocker -DockerArguments @('info', '--format', '{{json .}}')) |
        ConvertFrom-Json
    if ($engine.OSType -ne 'linux') { throw 'Selezionare i container Linux in Docker Desktop.' }
    return $engine
}

function Assert-SingleDatabase {
    param([string]$Database)
    # Controllo per etichette, anche per DB dei task futuri gia attivi nel progetto.
    # docker ps include anche container paused/restarting: non vanno ignorati.
    foreach ($other in @('postgres', 'mongo', 'cockroach')) {
        if ($other -eq $Database) { continue }
        $active = @(Invoke-LabDocker -DockerArguments @('ps', '--quiet',
            '--filter', "label=com.docker.compose.project=$script:ProjectName",
            '--filter', "label=com.docker.compose.service=$other"))
        if ($active.Count -gt 0) {
            throw "Avvio rifiutato: $other e attivo nel progetto. Arrestarlo esplicitamente prima di avviare $Database."
        }
    }
}
