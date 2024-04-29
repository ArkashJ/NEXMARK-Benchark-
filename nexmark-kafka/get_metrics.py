import json
import os
import requests
from datetime import datetime

# Configuration
flink_host = "localhost"
flink_port = "8081"
job_id = "ff92c19cb1e6aceeb4dfc2baba79fe7d"  # Replace <jobId> with your actual job ID
output_dir = "/Volumes/ARION/flink-tool/temp/metrics"  # Directory to save the metrics file

metric_ids = [
    "latency.source_id.bc764cd8ddf7a0cff126f51c16239658.operator_id.51397532e2d9c7a21097a30d590b3114.operator_subtask_index.0.latency_median",
    "lastCheckpointDuration",
    "fullRestarts",
]

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)


# Function to fetch and save metric values
def fetch_and_save_metric_values(metric_ids):
    metrics_values = {}

    for metric_id in metric_ids:
        # Construct the URL to fetch each metric's value
        url = f"http://{flink_host}:{flink_port}/jobs/{job_id}/metrics?get={metric_id}"

        # Fetch metric value
        response = requests.get(url)
        if response.status_code == 200:
            metric_value = response.json()
            metrics_values[metric_id] = metric_value
        else:
            print(f"Failed to fetch metric {metric_id}. HTTP Status Code: {response.status_code}")

    # Save metrics values to file
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file_path = os.path.join(output_dir, f"metrics_values_{job_id}_{timestamp}.json")

    with open(output_file_path, 'w') as f:
        json.dump(metrics_values, f, indent=4)

    print(f"Metrics values saved to {output_file_path}")


fetch_and_save_metric_values(metric_ids)
