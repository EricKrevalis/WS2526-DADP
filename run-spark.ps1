# Spark Submit Helper Script
# Sets required environment variables and runs spark-submit

# Get the project root directory
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPath = Join-Path $projectRoot ".venv"

# Set Spark environment variables
$env:SPARK_HOME = Join-Path $venvPath "lib\site-packages\pyspark"
$env:PYSPARK_PYTHON = Join-Path $venvPath "Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = Join-Path $venvPath "Scripts\python.exe"

# Get spark-submit path
$sparkSubmit = Join-Path $env:SPARK_HOME "bin\spark-submit"

# Run the command with all arguments passed to this script
& $sparkSubmit $args

