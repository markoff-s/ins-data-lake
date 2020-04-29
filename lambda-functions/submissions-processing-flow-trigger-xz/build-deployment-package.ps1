#Set-ExecutionPolicy Unrestricted

Write-Host "Deleting old target dir"
rm -r .\target\

Write-Host "Deleting old zip archive"
rm overnight_flow_trigger.zip

Write-Host "Creating new target dir"
mkdir target

Write-Host "Copying overnight_flow_trigger.py to target dir"
cp .\overnight_flow_trigger.py .\target\

#Write-Host "Copying site-packages to target dir"
#cp -r .\venv\Lib\site-packages\* .\target\

Write-Host "Zipping target dir to overnight_flow_trigger.zip"
#compress-archive -path .\target\* -destinationpath .\target\overnight_flow_trigger.zip -update
$command = '"C:\Program Files\7-Zip\7z.exe" a overnight_flow_trigger.zip .\target\*'
Invoke-Expression "& $command"