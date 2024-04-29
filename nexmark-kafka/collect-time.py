import os
import re
from datetime import datetime


def calculate_time_difference(log_file_path):
    restore_checkpoint_pattern = re.compile(r"Restoring job .* from Checkpoint")
    node_running_pattern = re.compile(r"switched from INITIALIZING to RUNNING")

    last_restore_checkpoint_timestamp = None
    last_node_running_timestamp = None

    with open(log_file_path, 'r') as file:
        for line in file:
            if restore_checkpoint_pattern.search(line):
                last_restore_checkpoint_timestamp = datetime.strptime(line.split(',')[0], '%Y-%m-%d %H:%M:%S')
                restore_checkpoint_log_line = line
            if node_running_pattern.search(line):
                last_node_running_timestamp = datetime.strptime(line.split(',')[0], '%Y-%m-%d %H:%M:%S')
                node_running_log_line = line

    if last_restore_checkpoint_timestamp and last_node_running_timestamp:
        restore_checkpoint_milliseconds = int(line.split(',')[1].split(' ')[0])
        node_running_milliseconds = int(line.split(',')[1].split(' ')[0])
        total_microseconds_difference = (
                                                last_node_running_timestamp - last_restore_checkpoint_timestamp).total_seconds() * 1_000_000
        total_microseconds_difference += (node_running_milliseconds - restore_checkpoint_milliseconds) * 1000
        restore_checkpoint_milliseconds = int(restore_checkpoint_log_line.split(',')[1].split(' ')[0])
        node_running_milliseconds = int(node_running_log_line.split(',')[1].split(' ')[0])

        total_microseconds_difference = (8 * 1_000_000) + (
                (node_running_milliseconds - restore_checkpoint_milliseconds) * 1000)

        return total_microseconds_difference
    else:
        return None


def process_log_files(directory_path):
    results = {}
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".log"):
            file_path = os.path.join(directory_path, file_name)
            time_diff = calculate_time_difference(file_path)
            results[file_name] = time_diff
    return results


# Example usage
directory_path = '/Volumes/ARION/flink-tool/temp/flink-log'
results = process_log_files(directory_path)
for file_name, time_diff in results.items():
    print(f"{file_name}: {time_diff} microseconds")
