import requests
import os
import subprocess
import re
import random
import time

from kafka import KafkaConsumer

# Configuration
FLINK_JM_IP = "127.0.0.1"
FLINK_JM_PORT = "8081"
JAR_PATH_QUERY = "/Users/frankli/Documents/BU/CS551/Project/cloud-provider-benchmarks/nexmark-kafka/target/Query5-jar-with-dependencies.jar"
BROKER_ADDRESS = "127.0.0.1:9092"
FLINK_PATH = "/Users/frankli/Documents/flink_tool/flink-1.18.1"
TOPICS = ["query5_src", "query5_sink"]  # Kafka topics

# Utility Functions
def upload_jar_and_get_id(jar_path):
    """Upload JAR to Flink and return the job ID."""
    files = {'jarfile': (os.path.basename(jar_path), open(jar_path, 'rb'), 'application/x-java-archive')}
    response = requests.post(f"http://{FLINK_JM_IP}:{FLINK_JM_PORT}/jars/upload", files=files)
    response_data = response.json()
    return response_data['filename'].split('/')[-1]

def run_flink_job(jar_id, program_args):
    """Run a Flink job and return the job ID."""
    data = {
        "programArgs": program_args,
        "entryClass": "",
        "parallelism": "",
        "savepointPath": "",
        "allowNonRestoredState": False
    }
    response = requests.post(f"http://{FLINK_JM_IP}:{FLINK_JM_PORT}/jars/{jar_id}/run", json=data)
    response_data = response.json()
    print(response_data)
    return response_data['jobid']

def stop_flink_job(job_id):
    """Stop a Flink job using its job ID."""
    url = f"http://{FLINK_JM_IP}:{FLINK_JM_PORT}/jobs/{job_id}/yarn-cancel"
    response = requests.get(url)
    if response.status_code in [200, 202]:
        print(f"Successfully stopped the job: {job_id}")
    else:
        print(f"Failed to stop the job: {job_id}. Response: {response.text}")

def consume_kafka_data(topic):
    """Consume data from a specified Kafka topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER_ADDRESS],
        auto_offset_reset='earliest',  # Start reading at the earliest message
        group_id='my_consumer_group',  # Unique consumer group ID
        enable_auto_commit=True  # Automatically commit offsets
    )

    print(f"Starting to consume data from {topic}")
    for message in consumer:
        # Process each message
        print(f"Received message: {message.value}")
        # Break or continue based on your condition
        # For demonstration, we'll just print one message and break
        break

    consumer.close()


def find_and_kill_random_taskmanager():
    try:
        # List processes and grep for TaskManager
        ps_output = subprocess.check_output(["ps", "aux"]).decode('utf-8')
        # Use regex to find processes that match the TaskManager pattern
        pids = re.findall(r'\b\d+\b.*org.apache.flink.runtime.taskexecutor.TaskExecutor', ps_output)
        # Extract PIDs
        pids = [int(pid.split()[1]) for pid in pids]

        if pids:
            # Randomly select a TaskManager PID to kill
            pid_to_kill = random.choice(pids)
            print(f"Killing TaskManager with PID: {pid_to_kill}")
            # Kill the selected TaskManager process
            subprocess.run(["kill", str(pid_to_kill)], check=True)
            print("TaskManager killed successfully.")
        else:
            print("No TaskManager processes found.")

    except subprocess.CalledProcessError as e:
        print(f"Error during process operation: {e}")


# Main Workflow
if __name__ == "__main__":

    print("Consuming data from Kafka topics...")
    # Consume data from Kafka topic
    consume_kafka_data(TOPICS[0])

    print("Starting Query5 job...")
    # Upload and Start Query5 Flink Job
    jar_id_query = upload_jar_and_get_id(JAR_PATH_QUERY)
    query_job_id = run_flink_job(jar_id_query, f"--broker 127.0.0.1:9092 --src-topic {TOPICS[0]} --sink-topic {TOPICS[1]} --kafka-group 2")

    print("Query5 job started. Waiting for 10 minutes before stopping...")

    print("Waiting for 2 minutes before attempting to kill a random TaskManager...")
    time.sleep(2*60)  # 3 minutes

    subprocess.run(["/Users/frankli/Documents/flink_tool/flink-1.18.1/bin/taskmanager.sh stop"],shell=True, check=True)

    time.sleep(3*60)  # Waiting for 10 minutes

    # Step 3: Stop the Query5 job
    stop_flink_job(query_job_id)
    print("Query5 job stopped.")
