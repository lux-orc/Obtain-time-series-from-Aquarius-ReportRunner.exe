
# https://github.com/AquaticInformatics/getting-started/releases/ReportRunner
# Measure-Command {.\check_flow_availability.ps1 | Out-Default}  # Check the time to run


function Get-TimeSeries {
    param (
        [string]$site,
        [string]$exe,
        [string]$json,
        [string]$out_folder = '.',
        [string]$format = 'csv'
    )
    if (!(Test-Path $out_folder -PathType Container)) {
        New-Item -ItemType Directory -Force -Path $out_folder | Out-Null
        Write-Host "`n`nFolder [$out_folder] has been created!" -ForegroundColor Yellow
    }
    $dec_str = '===='
    $fo_name = @($site.Replace('/', '$'), $format) -join, '.'  # Updated on 2024-05-08
    $fo_path = @($out_folder, $fo_name) -join, '/'
    $msg = "`n" + @($dec_str, $site, $dec_str) -join, ' '
    Write-Host $msg -ForegroundColor Magenta
    $json_path = Get-ChildItem $json
    $json_name = $json_path.Name
    $arg_list = @(
        , '-server=https://aquarius.orc.govt.nz'
        , '-username=api-read'
        , '-password=PR98U3SKOczINoPHo7WM'
        , $json
        , "-ReportType=$format"
        , "-Description=$json_name"
        , "-ReportTitle=$site"
        , "-TimeSeries=$site"
        , "-SaveOutputFile=$fo_path"
    ) -join, ' '
    Start-Process -Wait -NoNewWindow -FilePath $exe -ArgumentList $arg_list
}
