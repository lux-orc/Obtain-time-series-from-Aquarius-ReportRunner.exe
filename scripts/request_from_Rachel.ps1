
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
# '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\tmp.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




$sites = @(
    , 'a1472e21c8ad49ff8ba04630803d2d24'  # EW387
    , '470e1cea630148eeb1aaf6baa9aa41ef'  # EW734
    , 'd12b64f697e842d09a43073bef49c813'  # EX154
    , 'd27ea99b98274a40a85aab50cc5adfe3'  # FG697
    , '30e1327c88364c6dbcc5028212663a82'  # FA780
    , '10a1d22dec7747869f7748ca88963363'  # EM481
    , '161e43e04f604899b16689cfc381e50a'  # EM287
    , 'aa60d37465ca496382a254c0fd5456b2'  # EM759
    , 'faa5af46e19a44469172790115c67e54'  # EM469
    , '30031b0ef2204c9e9d452544c0e6e710'  # FK317
    , '6952be9cc11f4e9aacd041da0079e41b'  # EM567
    , '9f598e7d261b41588bba4c4cd446e081'  # EM206
    , '6ed72c5e19e84e23b959b106eab023cc'  # EM218
    , 'f57558d1f23840cc888fcf3be355ec78'  # EM893
    , '1900536119ea4c16bb26a8891820737e'  # EM131
    , '46a86c156642452683eba50deb64fa33'  # EM617
    , '2da737e9cf824451b9c49d703108b6e8'  # EM462
    , '7deb0f326c6a47e59b756231224479a3'  # EM209
    , '71568f4180484ec79ade7088942d8338'  # EX666
    , '33267791ad0a4c31b9af03062cda978d'  # EM639
    , 'e2b068f47d564e5d9f0000b368d9c0b2'  # EM220
    , '41e5290a2ae34641b6e3418310e31d73'  # EN203
    , '0035777c31824272b1c92bbcac4d8c0a'  # EM135
    , '026a2b77a9f74976b4a5c948bf0d7393'  # EY199
    , '19d066b6bcd54ecb80f82bdc06804639'  # EM631
    , 'fbc3711faf9c4c47a976dfc5499987cc'  # EM195
    , 'f274e82671fa4f2f8b996c4e75dca5e1'  # EM136
    , 'cc2977cebffe42c1b629dda51d35638b'  # EM196
    , 'adb2d34468df4729ba361b24131e4f2b'  # EM869
    , '7571e8bc8b7f4faa9202dba8bafd8580'  # EM837
    , '57f8535a3e8a4570b3543fde92ba6dc8'  # EM201
    , '0e0377e4e7cd4f08a8fea90e698f2564'  # EM200
    , 'b573f9d4e5654f7d93833cc70ae346ea'  # EM221
    , '5997edaf7b734cb4b7a7076979ab73d6'  # EM212
    , '186b4eae69cb45a4871b6dde058ae708'  # EM161
    , 'c1909e267ede4e4bbd84541556e91585'  # EM211
    , '79b45dad89654c07b823ecb1eb8123e4'  # EN348
    , '53dd20271e514d3fbad01c92a00bbf80'  # EM289
    , 'ab77bb7947d24761844beca6f67cdee4'  # EM295
    , 'cceb55ac04fe46209dc01a68ca64b590'  # EM306
    , '03ef37500a9c4d7aad9fc01c0b928a4e'  # EM495
    , '07f6efae44614ed6b979dae6c79352b4'  # EM215
    , '379d44806990458e80f8628efc80b291'  # EM156
    , 'e59cd4b8d81549629628be76691f7e21'  # EM293
    , '1044e75adafd405787d0b22ed6df8f35'  # EM140
    , '44ea0d6a35044e6dba3177fa1fcf9a13'  # EM142
    , '777be05460034076b1c558e366e221c5'  # EM144
    , 'aa8c6d2c12214c66896c84b61e378b88'  # EM145
    , 'e9081aa36ba14daaadba604cad3aca12'  # EN047
    , '4e58c5fb44cd47bf81f2e230050023e5'  # EM182
    , '9f709f5734ae47e4b0c857e542e0af78'  # EM618
    , '76bdf3cf01f64a0390b0b1c90f60c4a9'  # EM764
    , '4b6d16acfc784c78b2d3912e4b0b438c'  # EY527
    , '345378a43ed54f6cb22587ff50414bfc'  # EM525
    , '0b3408079e1c44cc901e0ab10815f59d'  # EM151
    , '06f9d6cd10484983b5a37265c3224edf'  # EN063
    , '59e6172e953e451ab1c8a7584dcb5813'  # EM294
    , 'e9b622b379d84513b6f5a83a14e40cce'  # EN422
)
foreach ($site in $sites) {
    Get-TimeSeries `
        -site $site `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dFlo'
}




$msg = "`nThe end of running！`n"
Write-Host $msg -ForegroundColor Green
