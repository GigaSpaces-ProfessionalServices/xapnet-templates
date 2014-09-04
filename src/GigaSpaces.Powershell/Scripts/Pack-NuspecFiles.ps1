param([string]$xapVersion, [string]$xapPath)
. ./Includes/log.ps1

if([string]::IsNullOrWhiteSpace($xapVersion) -or [string]::IsNullOrWhiteSpace($xapPath)){
	log "error" "Example usage: Pack-NuspecFiles 1.2.3.4 C:\GigaSpaces\XAP.NET 10.0.0 x64\NET v4.0"
	exit
}

$nuspecFile = "../Nuget/GigaSpaces.Core.nuspec"
$nugetPackage = $([string]::Format("GigaSpaces.Core.{0}.nupkg", $xapVersion))

log "info" $([string]::Format("################## Packing XAP.NET {0} Nuget Specification Files ##################",$xapVersion))
log "info" $([string]::Format("NuGet Specification File: {0}", $nuspecFile))

$qualifiedPath = Resolve-Path $nuspecFile
$xmlDoc = [System.Xml.XmlDocument](Get-Content $nuspecFile)

log "info" "Updating nuspec version."
$xmlDoc.package.metadata.version = $xapVersion

log "info" "Including libraries"

$files = $xmlDoc.CreateElement("files")

$fileElement = $xmlDoc.CreateElement("file")
$fileElement.SetAttribute("src", $([string]::Format("{0}\bin\GigaSpaces.Core.dll", $xapPath)))
$fileElement.SetAttribute("target", "lib")

$fileElement1 = $xmlDoc.CreateElement("file")
$fileElement1.SetAttribute("src", $([string]::Format("{0}\bin\GigaSpaces.NetToJava.dll", $xapPath))) 
$fileElement1.SetAttribute("target", "lib")

$files.AppendChild($fileElement)
$files.AppendChild($fileElement1)
$xmlDoc.package.AppendChild($files)

$xmlDoc.Save($qualifiedPath);

../tools/nuget.exe pack $nuspecFile

log "info" $([string]::Format("Copying file {0} to nuget repository.", $nugetPackage)) 
Copy-Item $nugetPackage $([string]::Format("{0}\nuget\", $xapPath))

log "info" "Removing temporary package file."
Remove-Item $nugetPackage