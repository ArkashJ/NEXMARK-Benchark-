import subprocess
import requests
import os
import time
from kafka import KafkaConsumer

# Configuration

KAFKA_PATH = "/Users/arkashjain/kafka_2.12-3.3.1"
BROKER_ADDRESS = "127.0.0.1:9092"
FLINK_JM_IP = "127.0.0.1"
FLINK_JM_PORT = "8081"
JAR_PATH_SOURCE = "/Users/arkashjain/Desktop/Spring2024/CS551/cloud-provider-benchmarks/nexmark-kafka/target/KafkaSourceBid-jar-with-dependencies.jar"
TOPICS = ["query5_src", "query5_sink"]
RATELIST = "25000_300000"

# Kafka consumer configuration
CONSUMER_TOPIC = "query5_src"
OUTPUT_FILE_PATH = "kafka_data_output.txt"
# MAX_DATA_SIZE_GB = 1.5

# Utility Functions
def run_command(command):
    """Run shell commands."""
    try:
        subprocess.run(command, check=True, shell=False)
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
    return response_data.get('jobid')

def stop_flink_job(job_id):
    """Stop a Flink job using its job ID."""
    url = f"http://{FLINK_JM_IP}:{FLINK_JM_PORT}/jobs/{job_id}/yarn-cancel"
    response = requests.get(url)
    if response.status_code in [200, 202]:
        print(f"Successfully stopped the job: {job_id}")
    else:
        print(f"Failed to stop the job: {job_id}. Response: {response.text}")



def collect_data_from_kafka(topic, output_file):
    """Collect data from a specified Kafka topic and write it to a file indefinitely until externally stopped."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER_ADDRESS],
        auto_offset_reset='earliest'
    )

    with open(output_file, 'w', encoding='utf-8') as file:
        for message in consumer:
            file.write(message.value.decode('utf-8') + '\n')

if __name__ == "__main__":
    # Kafka Topic Management
    for topic in TOPICS:
        manage_kafka_topics("--delete", topic)
        time.sleep(1)  # Wait a bit after deleting before creating
        manage_kafka_topics("--create", topic)

    # Upload and Start the Flink Job for Data Generation
    jar_id_source = upload_jar_and_get_id(JAR_PATH_SOURCE)
    job_id = run_flink_job(jar_id_source, f"--broker {BROKER_ADDRESS} --kafka-topic {TOPICS[0]} --ratelist {RATELIST}")

    print("Data generation job started. Collecting data from Kafka...")

    # Collect Data from Kafka
    collect_data_from_kafka(CONSUMER_TOPIC, OUTPUT_FILE_PATH)

    print("Data collection completed.")
