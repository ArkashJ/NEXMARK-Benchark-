import subprocess

# Define the variables
bin_directory = "/Users/frankli/Documents/BU/CS551/Project/cloud-provider-benchmarks/nexmark-kafka"  # Set the actual path to the bin directory
kill_time = "KILL_TIME_VALUE"  # Set the actual kill time value
joblist = "joblist_value"  # Set the actual job list value

# Build the command
command = f"{bin_directory}/kill-timer.sh {kill_time} {joblist}"

# Run the command in the background
process = subprocess.Popen(command, shell=True)

# The script is now running in the background, just like using '&' in Bash
