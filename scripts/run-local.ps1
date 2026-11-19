[CmdletBinding()]
param(
    [string]$PythonCommand = "python",
    [string]$JavaHome = "",
    [string]$HadoopHome = "",
    [string]$IcebergJar = "",
    [ValidateSet("parquet", "iceberg")][string]$Format = "parquet",
    [ValidateSet("baseline", "optimized")][string]$Mode = "baseline",
    [string]$RunId = ("local-" + (Get-Date -Format "yyyyMMddTHHmmss")),
    [string]$InputPath = "data/facebook_combined.txt.gz",
    [string]$OutputPath = "artifacts/local",
    [string]$Master = "local[2]",
    [int]$Partitions = 4,
    [switch]$DisableAdaptiveCoalesce,
    [long]$AdaptiveAdvisoryPartitionBytes = 0,
    [switch]$AdaptiveCoalesceSizeFirst,
    [int]$TopK = 10,
    [long]$MaxWedges = 50000000,
    [ValidateSet("", "clean", "score")][string]$FailAfter = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExecutable = (Get-Command -Name $PythonCommand -CommandType Application -ErrorAction Stop).Source
$managedVariables = @("JAVA_HOME", "HADOOP_HOME", "PATH", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "PYSPARK_SUBMIT_ARGS", "SPARK_LOCAL_IP")
$previousEnvironment = @{}
foreach ($variableName in $managedVariables) {
    $previousEnvironment[$variableName] = [Environment]::GetEnvironmentVariable($variableName, "Process")
}

Push-Location -LiteralPath $projectRoot
try {
    if ($JavaHome) {
        $resolvedJava = (Resolve-Path -LiteralPath $JavaHome).Path
        if (-not (Test-Path -LiteralPath (Join-Path $resolvedJava "bin/java.exe"))) {
            throw "JavaHome must contain bin/java.exe (Java 17 recommended)."
        }
        $env:JAVA_HOME = $resolvedJava
        $env:PATH = (Join-Path $resolvedJava "bin") + [IO.Path]::PathSeparator + $env:PATH
    }
    if ($HadoopHome) {
        $resolvedHadoop = (Resolve-Path -LiteralPath $HadoopHome).Path
        $env:HADOOP_HOME = $resolvedHadoop
        $env:PATH = (Join-Path $resolvedHadoop "bin") + [IO.Path]::PathSeparator + $env:PATH
    }
    $env:PYSPARK_PYTHON = $pythonExecutable
    $env:PYSPARK_DRIVER_PYTHON = $pythonExecutable
    if ($Master -like "local*") { $env:SPARK_LOCAL_IP = "127.0.0.1" }
    $pipelineArguments = @("src/spark_pipeline.py", "--input", $InputPath, "--output", $OutputPath,
                          "--run-id", $RunId, "--master", $Master, "--partitions", "$Partitions",
                          "--k", "$TopK", "--max-wedges", "$MaxWedges", "--format", $Format, "--mode", $Mode)
    if ($DisableAdaptiveCoalesce) { $pipelineArguments += "--disable-adaptive-coalesce" }
    if ($AdaptiveAdvisoryPartitionBytes -lt 0) { throw "AdaptiveAdvisoryPartitionBytes must be positive, or zero to retain Spark defaults." }
    if ($AdaptiveAdvisoryPartitionBytes -gt 0) { $pipelineArguments += @("--adaptive-advisory-partition-bytes", "$AdaptiveAdvisoryPartitionBytes") }
    if ($AdaptiveCoalesceSizeFirst) { $pipelineArguments += "--adaptive-coalesce-size-first" }
    if ($IcebergJar) {
        $resolvedJar = (Resolve-Path -LiteralPath $IcebergJar).Path.Replace("\", "/")
        # PySpark parses this string with shlex; forward slashes and quoting
        # preserve Windows paths containing spaces. No shell evaluation occurs.
        $env:PYSPARK_SUBMIT_ARGS = '--jars "' + $resolvedJar + '" pyspark-shell'
        $pipelineArguments += "--iceberg-package="
    }
    if ($FailAfter) { $pipelineArguments += @("--fail-after", $FailAfter) }
    & $pythonExecutable @pipelineArguments
    if ($LASTEXITCODE -ne 0) { throw "Spark pipeline exited with code $LASTEXITCODE. Inspect the unpublished staging failure.json and event log." }
}
finally {
    foreach ($variableName in $managedVariables) {
        [Environment]::SetEnvironmentVariable($variableName, $previousEnvironment[$variableName], "Process")
    }
    Pop-Location
}
