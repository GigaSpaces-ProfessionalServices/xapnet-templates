# Skyler Severns
# Gigaspaces Technologies, Inc.
# skyler@gigaspaces.com

param([string]$xapVersion, [string]$xapPath)
. ./Includes/log.ps1

if([string]::IsNullOrWhiteSpace($xapVersion) -or [string]::IsNullOrWhiteSpace($xapPath)){
	log "error" "Example usage: Install-NugetRepo 1.2.3.4 C:\GigaSpaces\XAP.NET 10.0.0 x64\NET v4.0"
	exit
}

$xapNugetFolder = $([string]::Format("{0}\nuget\", $xapPath))

if(![System.IO.Directory]::Exists($xapNugetFolder)){
	new-item $xapNugetFolder -itemtype directory
}

$nuGetConfigFile = $env:APPDATA + "\Nuget\Nuget.config"

log "info" $([string]::Format("################## Installing XAP.NET {0} Nuget Repository ##################",$xapVersion))
log "info" $([string]::Format("XAP.NET Installation Path: {0}", $xapPath))

if(![System.IO.File]::Exists($nuGetConfigFile)){
	log "error" $([string]::Format("Could not find NuGet configuration file. [{0}]", $nuGetConfigFile))
	exit
} else {
	log "info" "Nuget configuration file successfully found."
}

$nuGetDocument = [System.Xml.XmlDocument](Get-Content $nuGetConfigFile)

$newRepositoryElement = $nuGetDocument.CreateElement("add")
$newRepositoryElement.SetAttribute("key", $([string]::Format("GigaSpaces XAP.NET {0}", $xapVersion)))
$newRepositoryElement.SetAttribute("value", $xapNugetFolder)

$packageSources = $nuGetDocument.configuration.packagesources;

if($packageSources -eq $null){
	log "info" "Creating packagesources XML element."
	$packageSources = $nuGetDocument.CreateElement("packageSources")
	$nuGetDocument.configuration.AppendChild($packageSources)
}

$packageSources.AppendChild($newRepositoryElement)
$nuGetDocument.Save($nuGetConfigFile)

# TODO: Stop this from logging successful every time.
log "info" "Update successfully completed!"

 