import subprocess
import requests
import os
import time

# Configuration
KAFKA_PATH = "/Users/arkashjain/kafka_2.12-3.3.1"
BROKER_ADDRESS = "127.0.0.1:9092"
ZOOKEEPER_START_SCRIPT = f"/Users/arkashjain/kafka_2.12-3.3.1/bin/zookeeper-server-start.sh"
ZOOKEEPER_CONFIG = f"${KAFKA_PATH}/config/zookeeper.properties"
KAFKA_START_SCRIPT = f"${KAFKA_PATH}/bin/kafka-server-start.sh"
KAFKA_CONFIG = f"${KAFKA_PATH}/config/server.properties"
TOPICS = ["query5_src", "query5_sink"]
FLINK_JM_IP = "127.0.0.1"
FLINK_JM_PORT = "8081"
JAR_PATH_SOURCE = "/Users/arkashjain/Desktop/Spring2024/CS551/cloud-provider-benchmarks/nexmark-kafka/target/KafkaSourceBid-jar-with-dependencies.jar"
JAR_PATH_QUERY = "/Users/arkashjain/Desktop/Spring2024/CS551/cloud-provider-benchmarks/nexmark-kafka/target/Query5-jar-with-dependencies.jar"
RATELIST = "500000_900000"

# Utility Functions
def run_command(command):
    """Run shell commands."""
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        raise e

def manage_kafka_topics(action, topic):
    """Create or delete Kafka topics."""
    if action not in ["--create", "--delete"]:
        raise ValueError("Invalid action for Kafka topic management.")
    command = [
        f"{KAFKA_PATH}/bin/kafka-topics.sh", action,
        "--bootstrap-server", BROKER_ADDRESS,
        "--topic", topic
    ]
    run_command(command)

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
    # Extract and return the job ID
    return response_data['jobid']

def stop_flink_job(job_id):
    """Stop a Flink job using its job ID."""
    url = f"http://{FLINK_JM_IP}:{FLINK_JM_PORT}/jobs/{job_id}/yarn-cancel"
    response = requests.get(url)
    if response.status_code in [200, 202]:
        print(f"Successfully stopped the job: {job_id}")
    else:
        print(f"Failed to stop the job: {job_id}. Response: {response.text}")



# Main Workflow
if __name__ == "__main__":
    # Kafka Topic Management
    for topic in TOPICS:
        manage_kafka_topics("--delete", topic)
        time.sleep(1)  # Wait a bit after deleting before creating
        manage_kafka_topics("--create", topic)

    # Upload and Start Flink Jobs
    jar_id_source = upload_jar_and_get_id(JAR_PATH_SOURCE)
    source_job_id = run_flink_job(jar_id_source, f"--broker {BROKER_ADDRESS} --kafka-topic {TOPICS[0]} --ratelist {RATELIST}")

    jar_id_query = upload_jar_and_get_id(JAR_PATH_QUERY)
    query_job_id = run_flink_job(jar_id_query, f"--broker {BROKER_ADDRESS} --src-topic {TOPICS[0]} --sink-topic {TOPICS[1]} --kafka-group 0")

    print("Job started. Waiting for 2 minutes before stopping...")
    time.sleep(2 * 60)  # 20 minutes in seconds

    # Stop the job
    stop_flink_job(source_job_id)
    print("Source stopped.")

    stop_flink_job(query_job_id)
    print("Query stopped.")

