#!/bin/bash

# Define the log file path
log_file="./jobmanager_log.txt"
# Output file for time differences
output_file="time_diffs.txt"

# Prepare/clean the output file
> "$output_file"

# Grep pattern to identify the checkpoint restoration
start_pattern="Restoring job"

# Grep pattern to identify transitions from INITIALIZING to RUNNING
end_pattern="switched from INITIALIZING to RUNNING"

# Extract the timestamp for the checkpoint restoration event
start_time=$(grep "$start_pattern" "$log_file" | awk '{print $1 " " $2}' | sed 's/,//')

# Define the date format
date_format="%Y-%m-%d %H:%M:%S"

# Convert the checkpoint restoration time to seconds since the epoch
start_sec=$(date -j -f "$date_format" "$start_time" +"%s")

# Process each "INITIALIZING to RUNNING" transition
grep "$end_pattern" "$log_file" | while read -r line; do
    end_time=$(echo "$line" | awk '{print $1 " " $2}' | sed 's/,//')
    end_sec=$(date -j -f "$date_format" "$end_time" +"%s")
    
    # Calculate the difference in seconds
    diff_sec=$((end_sec-start_sec))
    
    # Write the time difference to the output file
    echo "$diff_sec" >> "$output_file"
done
