#!/bin/bash

# Check if the required arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <KILL_TIME> <QUERY_ID>"
    exit 1
fi

# Assign provided arguments to variables
KILL_TIME=$1
QUERY_ID=$2

echo "Kill the task manager after $KILL_TIME seconds"
echo "Query ID: $QUERY_ID"

# Wait for the specified time before killing the task manager
sleep "$KILL_TIME"

# Run the Python script to kill the task manager
python3 "$(pwd)"/kill.py "$QUERY_ID" "$(pwd)"
