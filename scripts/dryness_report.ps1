
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\dryness_report.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites
$Flow_NumberPlate = @(
    , '44ea0d6a35044e6dba3177fa1fcf9a13'
    , '53dd20271e514d3fbad01c92a00bbf80'
    , '1044e75adafd405787d0b22ed6df8f35'
    , '9f598e7d261b41588bba4c4cd446e081'
    , 'cc2977cebffe42c1b629dda51d35638b'
    , '0e0377e4e7cd4f08a8fea90e698f2564'
    , '379d44806990458e80f8628efc80b291'
    , 'fbc3711faf9c4c47a976dfc5499987cc'
    , '30e1327c88364c6dbcc5028212663a82'
    , 'f274e82671fa4f2f8b996c4e75dca5e1'
    , '1900536119ea4c16bb26a8891820737e'
    , '4e58c5fb44cd47bf81f2e230050023e5'
    , '777be05460034076b1c558e366e221c5'
    , '186b4eae69cb45a4871b6dde058ae708'
    , '5997edaf7b734cb4b7a7076979ab73d6'
    , 'e59cd4b8d81549629628be76691f7e21'
    , '0b3408079e1c44cc901e0ab10815f59d'
    , 'cceb55ac04fe46209dc01a68ca64b590'
    , '0035777c31824272b1c92bbcac4d8c0a'
    , '83369aa02e7d4aaf80dcfc9ba33fd71f'
    , '359565ab937d4c19a9931eec24b69acf'
    , '2da737e9cf824451b9c49d703108b6e8'
    , 'faa5af46e19a44469172790115c67e54'
    , 'ab77bb7947d24761844beca6f67cdee4'
    , '59e6172e953e451ab1c8a7584dcb5813'
    , 'aa8c6d2c12214c66896c84b61e378b88'
    , '10a1d22dec7747869f7748ca88963363'
    , '03ef37500a9c4d7aad9fc01c0b928a4e'
    , 'aa60d37465ca496382a254c0fd5456b2'
    , '345378a43ed54f6cb22587ff50414bfc'
    , 'b92b2a1a2efb408d86598e991a0f9fc7'
    , '46a86c156642452683eba50deb64fa33'
    , '9f709f5734ae47e4b0c857e542e0af78'
    , '19d066b6bcd54ecb80f82bdc06804639'
    , '76bdf3cf01f64a0390b0b1c90f60c4a9'
    , 'f2cdec86f3534bda8725aaef2c3721f4'
    , '06f9d6cd10484983b5a37265c3224edf'
    , '7571e8bc8b7f4faa9202dba8bafd8580'
    , '41e5290a2ae34641b6e3418310e31d73'
    , '68e618731d34446f8455a09f32650718'
    , 'e9081aa36ba14daaadba604cad3aca12'
    , 'e9b622b379d84513b6f5a83a14e40cce'
    , 'a1472e21c8ad49ff8ba04630803d2d24'
    , '470e1cea630148eeb1aaf6baa9aa41ef'
    , 'd12b64f697e842d09a43073bef49c813'
    , '71568f4180484ec79ade7088942d8338'
    , 'f57558d1f23840cc888fcf3be355ec78'
    , '4e3e2302f2ad4c68bb4e591800ab323e'
    , '026a2b77a9f74976b4a5c948bf0d7393'
    , '79b45dad89654c07b823ecb1eb8123e4'
    , '4b6d16acfc784c78b2d3912e4b0b438c'
    , 'adb2d34468df4729ba361b24131e4f2b'
    , '6cfb3468bb2b41f0985e7395e59e5045'
    , '57f8535a3e8a4570b3543fde92ba6dc8'
    , 'c1909e267ede4e4bbd84541556e91585'
    , '07f6efae44614ed6b979dae6c79352b4'
)
foreach ($i in $Flow_NumberPlate) {
    Get-TimeSeries `
        -site $i `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dFlo'
}




# Rain gauges
$Rain_NumberPlate = @(
    , 'a14ce8300d694f65b5bc6b01beb0e457'
    , 'ac0b1d98b3674a8392c85fe3ffa9826f'
    , '2b6d6c6601664e579ffdaf2cabff5722'
    , 'fc1ccb8f9e7442aa8db26f7321d3d9d1'
    , '87da10985fb64e98ac18e37f5191aa89'
    , '5e8700bc31d64fd2a5eb3d63bf807438'
    , '1efff3701bb94a0a9f163d20c14b0ec2'
    , '77e733488b0a470cbf819c21e3bc32bc'
    , 'e73133075fc043d89e5c472636e75da5'
    , '916fff4e8f054f1dac5735393a0426b7'
    , '1026edd132fc468c840c5bd899399abe'
    , '0fb07f68b41345cd96faf98b055bcc11'
    , 'a36cbdea8dc040ce978cc521f1f9f069'
    , '5a2e1a5b04be49eda4478bd7e5a64221'
    , 'cc82270ee6964ba6bd79514716a95e73'
    , 'd92207517d434f1d8b391bb4a011c3a3'
    , '922b71d975e443b5a345a5fd62d1e404'
    , '9a035d0a639745cbaee218b637d8a864'
    , 'b0f69ecdd170428ca881a9be20b740ef'
    , 'a47be7f33fa84e69962ca405a28d9f92'
    , 'e00e4af9c4654b2b81ea58f1d0d9bea7'
    , '350ff3271da84bedab9600c8a7309d08'
    , '8f48991768e04e45bea894ed5a38e029'
    , '511a92bee0ac4c2cbb95aaf277c06be4'
    , '9014289ca22141c79e2a4658ec8bce15'
    , '9eb78d750d0e4385890889d75f930335'
    , '7f51fbd2ce9b4db695de655994941f60'
    , '02ce1647f62343718ba9b5b3d313460b'
    , '88210d4b08e24f3e912f25f368784a35'
    , 'e7444f0e38d84d94992b1cc75336329e'
    , '821bed59fd4f4df19ce2441967e068c9'
    , 'd1170c769e1d414bada16bed8f506000'
    , '0758e1f62c41404a96863259d566b97e'
    , 'ce0a3151028244fba10c8f851071aa60'
    , '9117cd3d7f1d45c78793e16067d306ee'
    , 'f762f4606ca2484ba115db4dbdc81afa'
    , '29fd19ef7b23461bb50dcb133abfc720'
    , 'cc474b1b165b44e79f141a3014235cc2'
    , '2350b47916844fbda659ac12345ddfb6'
)
foreach ($j in $Rain_NumberPlate) {
    Get-TimeSeries `
        -site $j `
        -exe $exe `
        -json '../report_setting/daily_sum.json' `
        -out_folder '../out/csv/dRain'
}




# Lake level sites
$LL_NumberPlate = @(
    , 'e6badf3f0d0745408e15f83213dfaced'
    , '7474ca23364e438c96c3bbc60eecbc01'
    , 'eb9a7111d47b453b961f7984b3d51377'
)
foreach ($k in $LL_NumberPlate) {
    Get-TimeSeries `
        -site $k `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dLL'
}




$msg = "`nThe script has run!`n"
Write-Host $msg -ForegroundColor Green
