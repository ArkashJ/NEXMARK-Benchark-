import os
import requests
import subprocess
import sys
import time
from kafka import KafkaConsumer

# Configuration
KAFKA_PATH = os.environ.get("KAFKA_PATH")
BROKER_ADDRESS = "127.0.0.1:9092"
FLINK_JM_IP = "127.0.0.1"
FLINK_JM_PORT = "8081"
RATELIST = "75000_900000"

TARGET_PATH = "/Users/frankli/Documents/BU/CS551/Project/cloud-provider-benchmarks/nexmark-kafka/target"

JAR_CONFIGS = {
    '3': {
        'auction': {
            'jar_path': os.path.join(TARGET_PATH, "KafkaSourceAuction-jar-with-dependencies.jar"),
            'topic': 'query3_a',
        },
        'person': {
            'jar_path': os.path.join(TARGET_PATH, "KafkaSourcePerson-jar-with-dependencies.jar"),
            'topic': 'query3_p',
        }
    },
    '5': {
        'jar_path': os.path.join(TARGET_PATH, "KafkaSourceBid-jar-with-dependencies.jar"),
        'topics': ['query5_src', 'query5_sink'],
    },
    '8': {
        'auction': {
            'jar_path': os.path.join(TARGET_PATH, "KafkaSourceAuction-jar-with-dependencies.jar"),
            'topic': 'query8_a',
        },
        'person': {
            'jar_path': os.path.join(TARGET_PATH, "KafkaSourcePerson-jar-with-dependencies.jar"),
            'topic': 'query8_p',
        }
    }
}


def run_command(command):
    """Run shell commands."""
    try:
        subprocess.run(command, check=True, shell=False)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        raise e


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


def collect_data_from_kafka(topic):
    """Collect data from a specified Kafka topic and write it to a file indefinitely until externally stopped."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER_ADDRESS],
        auto_offset_reset='earliest'
    )


def check_required_processes(process_names):
    """Check if the required processes are running."""
    try:
        result = subprocess.run(['jps'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8')

        # Check for each required process
        for name in process_names:
            if name not in output:
                print(f"Required process '{name}' is not running.")
                return False
        return True
    except Exception as e:
        print(f"Failed to execute 'jps'. Error: {e}")
        return False


def list_kafka_topics():
    """Returns a list of existing Kafka topics."""
    command = [
        f"{KAFKA_PATH}/bin/kafka-topics.sh",
        "--list",
        "--bootstrap-server", BROKER_ADDRESS
    ]
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
        topics = result.stdout.split('\n')
        return [topic.strip() for topic in topics if topic.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error listing Kafka topics: {e}")
        return []


def manage_kafka_topics(action, topic):
    """Create or delete Kafka topics, bypassing delete if the topic does not exist."""
    existing_topics = list_kafka_topics()
    if action == "--delete":
        if topic not in existing_topics:
            print(f"Topic '{topic}' does not exist. Skipping deletion.")
            return
        else:
            print(f"Deleting topic: {topic}")
    elif action == "--create":
        if topic in existing_topics:
            print(f"Topic '{topic}' already exists. Skipping creation.")
            return
        print(f"Creating topic: {topic}")

    command = [
        f"{KAFKA_PATH}/bin/kafka-topics.sh", action,
        "--bootstrap-server", BROKER_ADDRESS,
        "--topic", topic
    ]

    try:
        subprocess.run(command, check=True)
        if action == "--delete":
            print(f"Deleted topic: {topic}")
        elif action == "--create":
            print(f"Created topic: {topic}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command for topic '{topic}': {e}")


if __name__ == "__main__":

    query_selection = sys.argv[1]
    config = JAR_CONFIGS[query_selection]

    required_processes = ['Kafka', 'QuorumPeerMain', 'TaskManagerRunner', 'StandaloneSessionClusterEntrypoint']
    if not check_required_processes(required_processes):
        print("One or more required processes are not running. Please check your Kafka, Zookeeper, and Flink setup.")
        sys.exit(1)

    # Revised Kafka Topic Management for individual topics
    for key in config:
        topic_config = config[key]
        manage_kafka_topics("--delete", topic_config['topic'])
        time.sleep(1)  # Wait a bit after deleting before creating
        manage_kafka_topics("--create", topic_config['topic'])

    # Upload and Start the Flink Job for Each Data Stream
    for key in config:
        topic_config = config[key]
        jar_id = upload_jar_and_get_id(topic_config['jar_path'])
        job_id = run_flink_job(jar_id,
                               f"--broker {BROKER_ADDRESS} --kafka-topic {topic_config['topic']} --ratelist {RATELIST}")
        print(f"{key.capitalize()} data generation job started. Collecting data from Kafka...")

    time.sleep(15 * 60)

    print("Data collection completed.")
