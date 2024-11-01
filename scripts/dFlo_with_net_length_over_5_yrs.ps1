
# Measure-Command {.\check_flow_availability.ps1 | Out-Default}  # Check the time to run

Import-Module ../_tools/Get-TimeSeries.psm1
$exe = '../_tools/ReportRunner.exe'


# There are currently 79 flow sites with net length > 5 years (till 30 May 2024)
# The following are to obtain their full historic records (in daily basis)


# Flow sites (daily mean)
$sites = @(
    , 'e2b068f47d564e5d9f0000b368d9c0b2'
    , 'd49616e889394d80a2b55883c167e36f'
    , '9aae8435d2e842edb1a1bccd935ad8c0'
    , '17e012451f2648feab2679492e799351'
    , '7deb0f326c6a47e59b756231224479a3'
    , '07f6efae44614ed6b979dae6c79352b4'
    , '6ed72c5e19e84e23b959b106eab023cc'
    , '1044e75adafd405787d0b22ed6df8f35'
    , '472b1964dd654f9993d8a8e34bf0bc01'
    , 'c107ad74ae034c748c7f194a15001f91'
    , '57f8535a3e8a4570b3543fde92ba6dc8'
    , '736beeb0e0a64ee7b1cab966067fa327'
    , '571a37713afa4b45a464b6fc24c8bd0c'
    , 'b902ebb1838e428d83b2466fc73f8e47'
    , '30e1327c88364c6dbcc5028212663a82'
    , '6787c81b7c37466f809ace035ecce7b6'
    , 'b573f9d4e5654f7d93833cc70ae346ea'
    , 'f274e82671fa4f2f8b996c4e75dca5e1'
    , 'a2b9b011b4cf42e2b08c627a2da132da'
    , '4b2811b4c9bd49059978823bad245df5'
    , '6dc9299cd5514f62918cdff8fba7f21b'
    , '603a2307cc7441a3a6f813559a476b1e'
    , '8eb3f64e67954b86a9601a0c186dc0f4'
    , '5997edaf7b734cb4b7a7076979ab73d6'
    , '33267791ad0a4c31b9af03062cda978d'
    , '89430df793d6430cbf2df2d353c9d2e1'
    , '0b3408079e1c44cc901e0ab10815f59d'
    , '2ee9e64df2b64834a86880e4a6379590'
    , '1e13297d90fb4018b8117df4bc41e6be'
    , '2efe0fbeb7054423b39d29faae0bc9bb'
    , '1ae869adb9cf4cde8789d178e6f1b169'
    , '02cc0e9c8d064e0b981ef55cedec7cb4'
    , 'e64e92a687cb40a3bfc269b6db9bfc45'
    , '9df7bd318bb44dd4922cfa256ac7b49d'
    , 'e5bfcf4e944b4e8bafdead18f133de55'
    , '59e6172e953e451ab1c8a7584dcb5813'
    , 'd28943f60170486abed09c926db5f85b'
    , '645af1b51c404c23885e1ecdf628b4e9'
    , 'e5a067cccf32441c9294c7a9ff82b686'
    , '223b598a7b634195b7bea232c7b183da'
    , '3c6da92b25fe444392e1339adc2cddc4'
    , 'b92b2a1a2efb408d86598e991a0f9fc7'
    , 'abc3a55b8e2a48eab94fbf0fe999fce2'
    , '46a86c156642452683eba50deb64fa33'
    , 'e60de45dc96d42f7979881efbd145b8a'
    , 'c8d8597728584f1a8bec1380a8319b31'
    , '76bdf3cf01f64a0390b0b1c90f60c4a9'
    , '9287ea0536f742659351d7e8d41940de'
    , 'ffe95a8eded742cc829243c4b3b3a71c'
    , '4d941ecb870a4a91b5d3060ba1334942'
    , 'd5bfd514c65349bcbae7580fbeeb9581'
    , '09fbb867daae46818c258c9e3f684917'
    , 'e74d0805439846f7b0c528989b9e9dd6'
    , '5afc12e8e8d846e887586f5a75e90223'
    , '7615942f4ace40bdb7ada05cc9cddc10'
    , 'e9081aa36ba14daaadba604cad3aca12'
    , 'b6c5f0bcdec24697bc0e3def492f1c96'
    , 'baa1db2b4ced42d78fe1324a3168395b'
    , 'a672b1f5a95344d39a7f6d746cb92157'
    , 'f3cfa6ed108b464faa0e920c54429659'
    , 'd12b64f697e842d09a43073bef49c813'
    , '04d9a41d190f453b8309c00a8cb27831'
    , 'f57558d1f23840cc888fcf3be355ec78'
    , 'fa3e5f1683604ab092c31fd328ad665f'
    , '44039d78654b47caac1e3861faf98cda'
    , '00f78d19f9774655ac313b9a5bb2822f'
    , '6db8ceca0556465ebd49d7cf6285142a'
    , '30e0e1dd0aa441deb33114934c1c191b'
    , '904feb1cbb7d46de93e2313ce7e3aa1c'
    , 'd09ab9d2047b4a4cb5500a477516f6e5'
    , '4f10cb96d8d14313812d27e06dce8409'
    , '907e683a30534e099bfa5cb2ca00b6b1'
    , '2edaecb0f0d04596ae0fb203d4d1028d'
    , '9ad8d85837b547f7aaf83dc050b7caf3'
    , '8c27961a284b4734a24f3cd6e81cac8c'
    , '46a27cded2a84fa8adb621cc45e5c3e9'
    , 'd27ea99b98274a40a85aab50cc5adfe3'
    , '3ffdafb86d284eb38cc6adbf7ea6b610'
    , '52ec93d5a1744a3e8967463e88ea3d52'
)
foreach ($site in $sites) {
    Get-TimeSeries `
        -site $site `
        -exe $exe `
        -json '../report_setting/daily_mean.json' `
        -out_folder '../out/csv/dFlo'
}




$msg = "`n运行结束！`n"
Write-Host $msg -ForegroundColor Green
