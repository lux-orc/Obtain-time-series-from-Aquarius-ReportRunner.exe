
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\dryness_report.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites
$Flow_NumberPlate = @(
    , '0def15435a52403099abcb85b2466933'
    , '30e1327c88364c6dbcc5028212663a82'
    , '10a1d22dec7747869f7748ca88963363'
    , '161e43e04f604899b16689cfc381e50a'
    , 'e64e92a687cb40a3bfc269b6db9bfc45'
    , '824d3e1d00cb4d4cac171cbe494b438b'
    , 'e64fa77d5705490b83c439747f13c765'
    , 'bbf294173246411a8baa68f75348c7ae'
    , 'aa60d37465ca496382a254c0fd5456b2'
    , '252972c431cb4597b3510414c24dd597'
    , 'faa5af46e19a44469172790115c67e54'
    , '122aa2750f4943c7b5712a9185628ff5'
    , '9f598e7d261b41588bba4c4cd446e081'
    , 'c4b1702924184dfdb09d6c376636008e'
    , '69ad05035e7b417c9a1b2d8e63a7e04f'
    , '6ed72c5e19e84e23b959b106eab023cc'
    , '1900536119ea4c16bb26a8891820737e'
    , '46a86c156642452683eba50deb64fa33'
    , '359565ab937d4c19a9931eec24b69acf'
    , '2da737e9cf824451b9c49d703108b6e8'
    , '7deb0f326c6a47e59b756231224479a3'
    , '0c6a6e9f9f9f4dbdab38577439b78a17'
    , 'aff030d3ae704032a3e2543304611326'
    , '42e8951c4c0844628b053488795b9cbb'
    , '33267791ad0a4c31b9af03062cda978d'
    , '67135294529d46e38dfb5cf6e072b298'
    , '0035777c31824272b1c92bbcac4d8c0a'
    , 'f2a94e382c1c40f1bd1fe1e53ab9e3b5'
    , 'fbc3711faf9c4c47a976dfc5499987cc'
    , 'f274e82671fa4f2f8b996c4e75dca5e1'
    , 'a72ec1cd157743018efe446b2ad6af0b'
    , 'cc2977cebffe42c1b629dda51d35638b'
    , 'ca4abed3b65d4e8c909ebfc85f735fdc'
    , '358a3ce9b824499a91ff1eecf85199b4'
    , '57f8535a3e8a4570b3543fde92ba6dc8'
    , '0e0377e4e7cd4f08a8fea90e698f2564'
    , 'b573f9d4e5654f7d93833cc70ae346ea'
    , '5997edaf7b734cb4b7a7076979ab73d6'
    , '186b4eae69cb45a4871b6dde058ae708'
    , 'c1909e267ede4e4bbd84541556e91585'
    , '53dd20271e514d3fbad01c92a00bbf80'
    , 'ab77bb7947d24761844beca6f67cdee4'
    , 'cceb55ac04fe46209dc01a68ca64b590'
    , '24e852405dfb423b8eb2bf724f28b40d'
    , '03ef37500a9c4d7aad9fc01c0b928a4e'
    , '5421c91ddcd946cd9cda006f5c5ae6af'
    , '83369aa02e7d4aaf80dcfc9ba33fd71f'
    , '07f6efae44614ed6b979dae6c79352b4'
    , 'b92b2a1a2efb408d86598e991a0f9fc7'
    , '277bd10a2bb04fff9c08a68a2e52cae4'
    , '379d44806990458e80f8628efc80b291'
    , '12e45f7dc23f41019209d1b58cf9554b'
    , '56e886d582f1466a8cbc57e93e31b446'
    , 'e59cd4b8d81549629628be76691f7e21'
    , 'd4c7f1d2a7a6481c8ed70fd4c111b653'
    , '1044e75adafd405787d0b22ed6df8f35'
    , '9356e2988df549aa89874490ae10d6e8'
    , '23c66c1afd6142e4b1f61156f31b25b3'
    , '44ea0d6a35044e6dba3177fa1fcf9a13'
    , '777be05460034076b1c558e366e221c5'
    , 'aa8c6d2c12214c66896c84b61e378b88'
    , '937eef0337a74b9e8a3e8c0ce798fdf0'
    , '4e58c5fb44cd47bf81f2e230050023e5'
    , '9f709f5734ae47e4b0c857e542e0af78'
    , '345378a43ed54f6cb22587ff50414bfc'
    , '0b3408079e1c44cc901e0ab10815f59d'
    , '59e6172e953e451ab1c8a7584dcb5813'
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
    , '511a92bee0ac4c2cbb95aaf277c06be4'
    , '8467368c8f074a058e184edd29129b0e'
    , 'fc1ccb8f9e7442aa8db26f7321d3d9d1'
    , 'cc50c99e49a54579ab6452a92a5792ac'
    , 'b754499c91cf4095b436a96b3c7687bc'
    , '8c4476cd85ec475aaaf136ac8ce077c4'
    , '26b59be24383436382065658134d2f31'
    , 'b5156d53a3664bffa8cc498073e5d5c6'
    , '1bd78b224dd44071a8d6972c63640259'
    , 'ad6ca7ab29bb4ddb8125bb592cb717cc'
    , 'd716220717f54f89b62fe63f2dc4afa6'
    , '89a0b3fbe1a94660aaad5b098ed5ab38'
    , '2fb68101d2274644941b373f5e4baebf'
    , 'b0f69ecdd170428ca881a9be20b740ef'
    , '7d719670f55149c8bea83b485c9b73d2'
    , '9014289ca22141c79e2a4658ec8bce15'
    , 'f66fb4dce3be41d69e5617961330f145'
    , 'cc82270ee6964ba6bd79514716a95e73'
    , 'f91052eca51a4bc58f5655b4fa9f07e5'
    , '5a2e1a5b04be49eda4478bd7e5a64221'
    , '29fd19ef7b23461bb50dcb133abfc720'
    , 'cffc53efa7664752b5f9a5030bce34e4'
    , '24a768fbbbaf4473a75cdf8664874f1d'
    , 'cb52baa935ea4122932abe0dd73a1ef3'
    , '18849e8f4647493e90c68aa05f17ab0c'
    , '77e733488b0a470cbf819c21e3bc32bc'
    , 'bd1ae8fc731b4622a2635fb26a0065e2'
    , 'a14ce8300d694f65b5bc6b01beb0e457'
    , '8db72632faa54103b1f3a7d4fde33310'
    , '8d6c6bf96b6142979eb00e50baa6436d'
    , '66c2fb56b91241b1b26a22ee9b8d5466'
    , 'f10dce291dd548999670efe99d3ee6cb'
    , 'd92207517d434f1d8b391bb4a011c3a3'
    , 'e00e4af9c4654b2b81ea58f1d0d9bea7'
    , 'acb16228f34c4b60877242d85567d965'
    , '6b9b3c7a841b40bc9968543bd8666db3'
    , '132c695278a841f28943db70b6557907'
    , '87da10985fb64e98ac18e37f5191aa89'
    , '70f2f9f2111a44e395aae9f960c7c712'
    , '9a035d0a639745cbaee218b637d8a864'
    , 'c4b9f3083ac748c4be533c73eaaf3681'
    , '9c5ce2735e3d44d6921fc64ddc689a0f'
    , '7c7af9119e48477bbd639771818f5b41'
    , '01c2ea5581c948b5a0edd64040989189'
    , '021f03b49acb4ddcab2851bfca956f6d'
    , 'a42a7910d4f04e16af7f3952ae7c4647'
    , '922b71d975e443b5a345a5fd62d1e404'
    , '69cfa57fafd4417287029649e1804fbd'
    , 'cd8c66740e2b46eda38eddf1527eecd7'
    , 'a47be7f33fa84e69962ca405a28d9f92'
    , '2350b47916844fbda659ac12345ddfb6'
    , '7f59fff086dc4c90a777f393ad3d223e'
    , '2b6d6c6601664e579ffdaf2cabff5722'
    , '49678c6071a345499898b3fa40a541a6'
    , '916fff4e8f054f1dac5735393a0426b7'
    , '22a95117a1e94e6ab0277d083aaea73c'
    , 'c1b79f2df6504dad845dae059e1e4fe8'
    , 'ae25d1a8b838477ba1a5c831cc17b126'
    , '3b42f18ca52c4f5fa2cd1a987836354d'
    , 'e73133075fc043d89e5c472636e75da5'
    , 'c9d85dceb02048f08274c1319308d9fb'
    , '5e8700bc31d64fd2a5eb3d63bf807438'
    , 'a47311c68ad242f1be108b77df4b12c0'
    , 'b78a15fadcac4520abd13896d0c51d00'
    , 'ac0b1d98b3674a8392c85fe3ffa9826f'
    , '9ed67fc356a040808ed361af1db70994'
    , 'd07591f4f9ce46b58a6c76cfecc1b705'
    , '350ff3271da84bedab9600c8a7309d08'
    , '9e0d94ac884841edb50b268a6e116f90'
    , '8f48991768e04e45bea894ed5a38e029'
    , '6c466f1bd1624aebb7483ca666706dcd'
    , '4bee91be6a8544daa93451962331b5f1'
    , '879bf2038f8f4e0dbc22136c5cb238c1'
    , 'e4e29eb767914854bb7b9d531b984ffb'
    , '9117cd3d7f1d45c78793e16067d306ee'
    , '00759378db414700a5d745fecd864e4d'
    , '4811e8a2523d43dd80af25b18c9c32a4'
    , '702f4a8cd1e94ba6bf8b4d3f1d24bc27'
    , 'f893ef849b754779a54d91281f35961f'
    , '1026edd132fc468c840c5bd899399abe'
    , '55c6ee15629e4222bcc5954914dcdaa9'
    , '7c15eb65701f4897aebc96692b7adf72'
    , 'ce0a3151028244fba10c8f851071aa60'
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
    , '8cf9f5bbc1c04e0aa0944616ad8e4798'
    , 'a20701ac6d9644a8b5619a91c0927543'
    , '0dfbaa772aaa41e1959ded9a15f666da'
    , 'dbf0ead7443248b3ab01d23c1bfbf9d9'
    , '7474ca23364e438c96c3bbc60eecbc01'
    , 'cfa49b49f8e54908bdb18ad761881faa'
    , 'fb4733ebc1f14244b7ae411961df7400'
    , 'eb9a7111d47b453b961f7984b3d51377'
    , 'e6badf3f0d0745408e15f83213dfaced'
)
foreach ($k in $LL_NumberPlate) {
    Get-TimeSeries `
        -site $k `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dLL'
}




# Others
$Others = @(
    , '1639dd4e3c314901902194058bc02d7c'
    , '48066a19e4054a3c8d55c0f53a55ba26'
    , 'f9d8ccb45679470a838180fc91b91d35'
    , '5ce7eade4c234e9e806e7b90932776a7'
    , '9734f438db6941a3b975ae459992cc7f'
    , 'ff91ecdb398048e485998b4f5f4bc926'
    , '9bda1caa904c4b25bfbeb237906c79e4'
    , 'a2e5f17c4c4d423b80d5c9784ea92879'
    , '5d23d499d89c4121b0f70447739381ae'
    , 'f63d3ba53ab4441c8224c2367704b40e'
    , '8976afd907d94c1ca74d679440fe18fc'
)
foreach ($l in $Others) {
    Get-TimeSeries `
        -site $l `
        -exe $exe `
        -json '../report_setting/raw.json' `
        -out_folder '../out/csv/raw_monthly'
}





$msg = "`nThe script has run!`n"
Write-Host $msg -ForegroundColor Green
