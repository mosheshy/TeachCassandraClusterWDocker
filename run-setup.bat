@echo off
:: Runs the PowerShell script to start the Cassandra Docker cluster

echo Starting Cassandra cluster using Docker Compose...
#!/bin/bash
# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/start-cassandra-cluster.sh"pause
