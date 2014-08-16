### Logs a message to the console.
### The log level: info, error
function log([string]$logLevel,[string]$message){
	$printLevel = "NOT DEFINED"

	if("info".Equals($logLevel, [StringComparison]::InvariantCultureIgnoreCase)){
		$printLevel = "INFO "
	} elseif("error".Equals($logLevel, [StringComparison]::InvariantCultureIgnoreCase)){
		$printLevel = "ERROR"
		[Console]::ForegroundColor = "Red"
	}

	[String]::Format("[{0}] [{1}] {2}", [DateTime]::Now.ToString("G"), $printLevel, $message)
	[Console]::ResetColor()
}