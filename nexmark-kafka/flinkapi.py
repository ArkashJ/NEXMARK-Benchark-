import os
import requests
import subprocess
import sys
import time

from jar_config import *


def run_command(command):
    """Run shell commands."""
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        raise e


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


def main(sOrq, query_id):
    grounp_num = 0

    if query_id not in QUERIES:
        print(f"Query {query_id} not found")
        return
    elif sOrq == 's':
        query_config = QUERIES[query_id]
        topics = query_config['topics']
        sources_jars = [JAR_PATH[source] for source in query_config['sources']]

        for topic in topics:
            manage_kafka_topics("--delete", topic)
            manage_kafka_topics("--create", topic)
        for jar_path in sources_jars:
            jar_id = upload_jar_and_get_id(jar_path)
            topic = topics[sources_jars.index(jar_path)]
            run_flink_job(jar_id, f"--broker {BROKER_ADDRESS} --kafka-topic {topic} --ratelist {RATELIST}")

        print("Start collecting source, waiting for 10 mintues before stopping...")
        time.sleep(600)
    elif sOrq == 'q':
        internval = 3000
        query_config = QUERIES[query_id]
        topics = query_config['topics']
        # sources_jars = [JAR_PATH[source] for source in query_config['sources']]

        query_jar_path = JAR_PATH[f"query{query_id}"]
        query_jar_id = upload_jar_and_get_id(query_jar_path)
        run_flink_job(query_jar_id, " ".join([f"--broker {BROKER_ADDRESS}", *[
            f"--{name}-kafka-topic {topic} --{name}-kafka-group {grounp_num}" for name, topic in
            zip(query_config['sources'], topics)
        ], f"--interval {internval}"]))
        print("Jobs started. Waiting for 2 minutes before stopping...")
        time.sleep(120)


# Run the script with the query ID as an argument
if __name__ == "__main__":
    print("current directory: ", os.getcwd())
    if len(sys.argv) != 3:
        print("Usage: python flinkapi.py <s/q> <query_id>")
        sys.exit(1)

    required_processes = ['Kafka', 'QuorumPeerMain', 'TaskManagerRunner', 'StandaloneSessionClusterEntrypoint']
    if not check_required_processes(required_processes):
        print("One or more required processes are not running. Please check your Kafka, Zookeeper, and Flink setup.")
        sys.exit(1)
    sOrq = sys.argv[1]
    query_id = sys.argv[2]
    main(sOrq, query_id)
