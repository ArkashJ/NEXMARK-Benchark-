# read_time_diffs.py

def read_time_diffs(file_path):
    """Read time differences from a file into a list."""
    with open(file_path, 'r') as file:
        # Each line in the file is a time difference, so convert each to int
        time_diffs = [int(line.strip()) for line in file]
    return time_diffs


# Path to the file containing time differences
file_path = 'time_diffs.txt'
time_diffs = read_time_diffs(file_path)

# Print the list of time differences
print("Time Differences:", time_diffs)
