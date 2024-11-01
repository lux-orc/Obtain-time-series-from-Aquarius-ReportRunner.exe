
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\check_flow_availability.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Items of interests (36)
$sites = @(
    , '511a92bee0ac4c2cbb95aaf277c06be4'
    , 'fc1ccb8f9e7442aa8db26f7321d3d9d1'
    , '2fb68101d2274644941b373f5e4baebf'
    , 'b0f69ecdd170428ca881a9be20b740ef'
    , '7d719670f55149c8bea83b485c9b73d2'
    , '9014289ca22141c79e2a4658ec8bce15'
    , 'cc82270ee6964ba6bd79514716a95e73'
    , '5a2e1a5b04be49eda4478bd7e5a64221'
    , '29fd19ef7b23461bb50dcb133abfc720'
    , '24a768fbbbaf4473a75cdf8664874f1d'
    , '77e733488b0a470cbf819c21e3bc32bc'
    , 'a14ce8300d694f65b5bc6b01beb0e457'
    , 'd92207517d434f1d8b391bb4a011c3a3'
    , 'e00e4af9c4654b2b81ea58f1d0d9bea7'
    , '87da10985fb64e98ac18e37f5191aa89'
    , '9a035d0a639745cbaee218b637d8a864'
    , '7c7af9119e48477bbd639771818f5b41'
    , '922b71d975e443b5a345a5fd62d1e404'
    , 'a47be7f33fa84e69962ca405a28d9f92'
    , '2350b47916844fbda659ac12345ddfb6'
    , '2b6d6c6601664e579ffdaf2cabff5722'
    , '916fff4e8f054f1dac5735393a0426b7'
    , 'ae25d1a8b838477ba1a5c831cc17b126'
    , 'e73133075fc043d89e5c472636e75da5'
    , '5e8700bc31d64fd2a5eb3d63bf807438'
    , 'ac0b1d98b3674a8392c85fe3ffa9826f'
    , '350ff3271da84bedab9600c8a7309d08'
    , '9e0d94ac884841edb50b268a6e116f90'
    , '8f48991768e04e45bea894ed5a38e029'
    , 'f762f4606ca2484ba115db4dbdc81afa'
    , '9117cd3d7f1d45c78793e16067d306ee'
    , '00759378db414700a5d745fecd864e4d'
    , '702f4a8cd1e94ba6bf8b4d3f1d24bc27'
    , '1026edd132fc468c840c5bd899399abe'
    , '7c15eb65701f4897aebc96692b7adf72'
    , 'ce0a3151028244fba10c8f851071aa60'
)
foreach ($site in $sites) {
    Get-TimeSeries `
        -site $site `
        -exe $exe `
        -json '../report_setting/daily_sum.json' `
        -out_folder '../out/csv/dRain'
}




$msg = "`n运行结束！`n"
Write-Host $msg -ForegroundColor Green
