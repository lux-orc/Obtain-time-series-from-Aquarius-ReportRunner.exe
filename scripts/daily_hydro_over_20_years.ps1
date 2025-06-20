
# stackoverflow.com/questions/67217049/passing-an-argument-list-with-spaces-in-powershell
    # '"Discharge Volume.Telemetry@EX331"'

# Measure-Command {.\dryness_report.ps1 | Out-Default}  # Check the time to run


Import-Module ../_tools/Get-TimeSeries.psm1

$exe = '../_tools/ReportRunner.exe'




# Flow sites
$Flow_NumberPlate = @(
    , 'a2b9b011b4cf42e2b08c627a2da132da'
    , '1c38e6f738e146f88581774965a50701'
    , '2efe0fbeb7054423b39d29faae0bc9bb'
    , 'f274e82671fa4f2f8b996c4e75dca5e1'
    , '1044e75adafd405787d0b22ed6df8f35'
    , '9aae8435d2e842edb1a1bccd935ad8c0'
    , '6dc9299cd5514f62918cdff8fba7f21b'
    , 'aa8c6d2c12214c66896c84b61e378b88'
    , '23c66c1afd6142e4b1f61156f31b25b3'
    , 'd4c7f1d2a7a6481c8ed70fd4c111b653'
    , '0b3408079e1c44cc901e0ab10815f59d'
    , '571a37713afa4b45a464b6fc24c8bd0c'
    , 'ca4abed3b65d4e8c909ebfc85f735fdc'
    , '603a2307cc7441a3a6f813559a476b1e'
    , 'f4efd8e9169c411c9b2a05282cd7bf2a'
    , 'b11f067bc1e9407da4b0c8d0cc817e04'
    , '252972c431cb4597b3510414c24dd597'
    , 'abd77b36118e41feb40efd6f26e7fd4d'
    , '937eef0337a74b9e8a3e8c0ce798fdf0'
    , '4b2811b4c9bd49059978823bad245df5'
    , '824d3e1d00cb4d4cac171cbe494b438b'
    , 'ddd961a59f29448e8a0c61b743280338'
    , 'b902ebb1838e428d83b2466fc73f8e47'
    , 'c107ad74ae034c748c7f194a15001f91'
    , '736beeb0e0a64ee7b1cab966067fa327'
    , '57f8535a3e8a4570b3543fde92ba6dc8'
    , 'ff69323c26984c96be61f10ce94f719b'
    , '9f598e7d261b41588bba4c4cd446e081'
    , '7deb0f326c6a47e59b756231224479a3'
    , 'c1909e267ede4e4bbd84541556e91585'
    , '5997edaf7b734cb4b7a7076979ab73d6'
    , '07f6efae44614ed6b979dae6c79352b4'
    , '6ed72c5e19e84e23b959b106eab023cc'
    , '0def15435a52403099abcb85b2466933'
    , 'e2b068f47d564e5d9f0000b368d9c0b2'
    , 'b573f9d4e5654f7d93833cc70ae346ea'
    , '4c0955702a034f3bb6a5128702fc4088'
    , '9356e2988df549aa89874490ae10d6e8'
    , 'd49616e889394d80a2b55883c167e36f'
    , '17e012451f2648feab2679492e799351'
    , '89430df793d6430cbf2df2d353c9d2e1'
    , '59e6172e953e451ab1c8a7584dcb5813'
    , 'e5bfcf4e944b4e8bafdead18f133de55'
    , '4d2e63f988f14ebda00e8fc8bf1869d3'
    , '2ee9e64df2b64834a86880e4a6379590'
    , '40c94cf5fb5e4c3a9a99bbec413c4ef9'
    , '1e13297d90fb4018b8117df4bc41e6be'
    , '1ae869adb9cf4cde8789d178e6f1b169'
    , 'e64e92a687cb40a3bfc269b6db9bfc45'
    , '2da737e9cf824451b9c49d703108b6e8'
    , 'faa5af46e19a44469172790115c67e54'
    , '10a1d22dec7747869f7748ca88963363'
    , 'e5a067cccf32441c9294c7a9ff82b686'
    , 'aff030d3ae704032a3e2543304611326'
    , '345378a43ed54f6cb22587ff50414bfc'
    , '8b0761142aea44ffa635a0836446602e'
    , 'b92b2a1a2efb408d86598e991a0f9fc7'
    , 'bbf294173246411a8baa68f75348c7ae'
    , '46a86c156642452683eba50deb64fa33'
    , 'e60de45dc96d42f7979881efbd145b8a'
    , '33267791ad0a4c31b9af03062cda978d'
    , 'a06f328b2e3d4f42a06856fde4ed265d'
    , '42e8951c4c0844628b053488795b9cbb'
    , 'd44149e7c0da4da4af697e61d712c235'
    , '223b598a7b634195b7bea232c7b183da'
    , '69ad05035e7b417c9a1b2d8e63a7e04f'
    , '30e1327c88364c6dbcc5028212663a82'
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
    , 'a1ad8c76bea6456d9e82e4e85e879f12'
    , 'ac0b1d98b3674a8392c85fe3ffa9826f'
    , '2b6d6c6601664e579ffdaf2cabff5722'
    , 'b78a15fadcac4520abd13896d0c51d00'
    , 'fc1ccb8f9e7442aa8db26f7321d3d9d1'
    , '9ed67fc356a040808ed361af1db70994'
    , '55c6ee15629e4222bcc5954914dcdaa9'
    , 'a42a7910d4f04e16af7f3952ae7c4647'
    , '87da10985fb64e98ac18e37f5191aa89'
    , '5e8700bc31d64fd2a5eb3d63bf807438'
    , '77e733488b0a470cbf819c21e3bc32bc'
    , 'e73133075fc043d89e5c472636e75da5'
    , '916fff4e8f054f1dac5735393a0426b7'
    , '1026edd132fc468c840c5bd899399abe'
    , '8976afd907d94c1ca74d679440fe18fc'
    , '7c7af9119e48477bbd639771818f5b41'
    , '879bf2038f8f4e0dbc22136c5cb238c1'
    , 'bd1ae8fc731b4622a2635fb26a0065e2'
    , '8db72632faa54103b1f3a7d4fde33310'
    , 'f66fb4dce3be41d69e5617961330f145'
    , '24a768fbbbaf4473a75cdf8664874f1d'
    , '8d6c6bf96b6142979eb00e50baa6436d'
    , '021f03b49acb4ddcab2851bfca956f6d'
    , '01bc9a9e601d4cfcb7ff3dba419e06dd'
    , '9c5ce2735e3d44d6921fc64ddc689a0f'
    , '9734f438db6941a3b975ae459992cc7f'
    , 'ad6ca7ab29bb4ddb8125bb592cb717cc'
    , 'ae25d1a8b838477ba1a5c831cc17b126'
    , '00759378db414700a5d745fecd864e4d'
    , '7d719670f55149c8bea83b485c9b73d2'
    , '89a0b3fbe1a94660aaad5b098ed5ab38'
    , '702f4a8cd1e94ba6bf8b4d3f1d24bc27'
    , 'e4e29eb767914854bb7b9d531b984ffb'
    , '6b9b3c7a841b40bc9968543bd8666db3'
    , '5a2e1a5b04be49eda4478bd7e5a64221'
    , '48066a19e4054a3c8d55c0f53a55ba26'
    , '9e0d94ac884841edb50b268a6e116f90'
    , 'cc82270ee6964ba6bd79514716a95e73'
    , 'f893ef849b754779a54d91281f35961f'
    , 'a43767cd798f4f6f975d3c07b69b36e0'
    , '66c2fb56b91241b1b26a22ee9b8d5466'
    , '26029927a2754c6a9efd3d55561f5537'
    , '8c4476cd85ec475aaaf136ac8ce077c4'
    , '70f2f9f2111a44e395aae9f960c7c712'
    , 'cffc53efa7664752b5f9a5030bce34e4'
    , '4bee91be6a8544daa93451962331b5f1'
    , 'd07591f4f9ce46b58a6c76cfecc1b705'
    , '132c695278a841f28943db70b6557907'
    , 'b5156d53a3664bffa8cc498073e5d5c6'
    , '7f59fff086dc4c90a777f393ad3d223e'
    , '1bd78b224dd44071a8d6972c63640259'
    , '4811e8a2523d43dd80af25b18c9c32a4'
    , 'c9d85dceb02048f08274c1319308d9fb'
    , '922b71d975e443b5a345a5fd62d1e404'
    , 'db8ef5f8c912438fabcecc5fbfa1cbcf'
    , 'b0f69ecdd170428ca881a9be20b740ef'
    , 'a47be7f33fa84e69962ca405a28d9f92'
    , '09a4b2df37fa4c1885133f1c74c32ffd'
    , 'e00e4af9c4654b2b81ea58f1d0d9bea7'
    , '350ff3271da84bedab9600c8a7309d08'
    , '3b42f18ca52c4f5fa2cd1a987836354d'
    , '8f48991768e04e45bea894ed5a38e029'
    , 'a47311c68ad242f1be108b77df4b12c0'
    , 'b754499c91cf4095b436a96b3c7687bc'
    , 'd716220717f54f89b62fe63f2dc4afa6'
    , '2fb68101d2274644941b373f5e4baebf'
    , '8467368c8f074a058e184edd29129b0e'
    , '18849e8f4647493e90c68aa05f17ab0c'
    , 'f9d8ccb45679470a838180fc91b91d35'
    , 'f10dce291dd548999670efe99d3ee6cb'
    , '69cfa57fafd4417287029649e1804fbd'
    , '511a92bee0ac4c2cbb95aaf277c06be4'
    , '9014289ca22141c79e2a4658ec8bce15'
    , 'f63d3ba53ab4441c8224c2367704b40e'
    , '1639dd4e3c314901902194058bc02d7c'
    , 'a2e5f17c4c4d423b80d5c9784ea92879'
    , 'ff91ecdb398048e485998b4f5f4bc926'
    , 'c4b9f3083ac748c4be533c73eaaf3681'
    , 'f91052eca51a4bc58f5655b4fa9f07e5'
    , '5d23d499d89c4121b0f70447739381ae'
    , '9bda1caa904c4b25bfbeb237906c79e4'
    , '5ce7eade4c234e9e806e7b90932776a7'
    , 'ce0a3151028244fba10c8f851071aa60'
    , '9117cd3d7f1d45c78793e16067d306ee'
    , '29fd19ef7b23461bb50dcb133abfc720'
    , '2350b47916844fbda659ac12345ddfb6'
    , 'cb52baa935ea4122932abe0dd73a1ef3'
    , 'c1b79f2df6504dad845dae059e1e4fe8'
    , 'acb16228f34c4b60877242d85567d965'
    , '6c466f1bd1624aebb7483ca666706dcd'
    , '49678c6071a345499898b3fa40a541a6'
    , '01c2ea5581c948b5a0edd64040989189'
    , 'cc50c99e49a54579ab6452a92a5792ac'
    , 'cd8c66740e2b46eda38eddf1527eecd7'
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
    , 'fb4733ebc1f14244b7ae411961df7400'
    , 'cfa49b49f8e54908bdb18ad761881faa'
    , 'a20701ac6d9644a8b5619a91c0927543'
    , 'e6badf3f0d0745408e15f83213dfaced'
    , '7474ca23364e438c96c3bbc60eecbc01'
    , '8cf9f5bbc1c04e0aa0944616ad8e4798'
    , '0dfbaa772aaa41e1959ded9a15f666da'
    , 'dbf0ead7443248b3ab01d23c1bfbf9d9'
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
