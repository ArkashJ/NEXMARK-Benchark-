import json
import os
import requests
import shutil
import subprocess
import sys
import time

# Configuration
FLINK_JM_IP = "127.0.0.1"
FLINK_JM_PORT = "8081"
BROKER_ADDRESS = "127.0.0.1:9092"
FLINK_PATH = os.environ.get("FLINK_PATH")

TARGET_PATH = "/Users/frankli/Documents/BU/CS551/Project/cloud-provider-benchmarks/nexmark-kafka/target"

QUERY_CONFIGS = {
    "3": {
        "jar_path": os.path.join(TARGET_PATH, "Query3-jar-with-dependencies.jar"),
        "program_args_template": "--broker 127.0.0.1:9092 --auction-kafka-topic {auction_topic} --auction-kafka-group {auction_group} --person-kafka-topic {person_topic} --person-kafka-group {person_group} --interval {interval} --p_join {p_join}",
        "auction_topic": "query3_a",
        "person_topic": "query3_p",
    },
    "5": {
        "jar_path": os.path.join(TARGET_PATH, "Query5-jar-with-dependencies.jar"),
        "program_args_template": "--broker 127.0.0.1:9092 --src-topic {src_topic} --sink-topic {sink_topic} --kafka-group {kafka_group} ----interval {interval} --p_join {p_join}",
        "src_topic": "query5_src",
        "sink_topic": "query5_sink",
    },
    "7": {
        "jar_path": os.path.join(TARGET_PATH, "Query7-jar-with-dependencies.jar"),
        "program_args_template": "--broker 127.0.0.1:9092 --auction-kafka-topic {auction_topic} --auction-kafka-group {auction_group} --person-kafka-topic {person_topic} --person-kafka-group {person_group} --bid-kafka-topic {bid_topic} --bid-kafka-group {bid_group} --interval {interval} --p-join {p_join}",
        "auction_topic": "query8_a",
        "person_topic": "query8_p",
        "bid_topic": "query8_b",
    },
    "8": {
        "jar_path": os.path.join(TARGET_PATH, "Query8-jar-with-dependencies.jar"),
        "program_args_template": "--broker 127.0.0.1:9092 --auction-kafka-topic {auction_topic} --auction-kafka-group {auction_group} --person-kafka-topic {person_topic} --person-kafka-group {person_group} --interval {interval} --p-join {p_join}",
        "auction_topic": "query8_a",
        "person_topic": "query8_p",
    }
}


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


def get_or_create_group_number(query_key,
                               base_path="/Users/frankli/Documents/BU/CS551/Project/cloud-provider-benchmarks/nexmark-kafka"):
    group_numbers_file = os.path.join(base_path, "group_numbers.json")

    # Check if the file exists and has content; if not, initialize it with an empty dictionary
    if not os.path.exists(group_numbers_file) or os.stat(group_numbers_file).st_size == 0:
        with open(group_numbers_file, "w") as file:
            json.dump({}, file)

    # Now, it's safe to load the file as it's guaranteed to contain valid JSON (an empty dictionary at the very least)
    with open(group_numbers_file, "r") as file:
        group_numbers = json.load(file)

    # Proceed with checking if the group number for the given query exists, and create it if not
    if query_key not in group_numbers:
        new_group_number = str(len(group_numbers) + 1)
        group_numbers[query_key] = new_group_number

        # Save the updated group numbers back to the file
        with open(group_numbers_file, "w") as file:
            json.dump(group_numbers, file)

    return group_numbers[query_key]


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


# Main Workflow
if __name__ == "__main__":

    if len(sys.argv) < 2 or sys.argv[1] not in QUERY_CONFIGS:
        print("Usage: python script.py <query3|query5|query8>")
        sys.exit(1)

    query_key = sys.argv[1]

    config = QUERY_CONFIGS[query_key]
    config['broker'] = "127.0.0.1:9092"  # Set or confirm the broker address

    required_processes = ['Kafka', 'QuorumPeerMain', 'TaskManagerRunner', 'StandaloneSessionClusterEntrypoint']
    if not check_required_processes(required_processes):
        print("One or more required processes are not running. Please check your Kafka, Zookeeper, and Flink setup.")
        sys.exit(1)

    for paralle in range(7, 13):
        # group_number = get_or_create_group_number(query_key)
        group_number = paralle + 17500
        print(f"Starting {query_key} job...")
        program_args = config['program_args_template'].format(
            broker=config['broker'],
            auction_topic=config.get('auction_topic', ''),
            auction_group=group_number,
            person_topic=config.get('person_topic', ''),
            person_group=group_number,
            # bid_topic=config.get('bid_topic', ''),
            # bid_group=group_number,
            src_topic=config.get('src_topic', ''),
            sink_topic=config.get('sink_topic', ''),
            kafka_group=group_number,
            interval=5000,
            p_join=paralle,
        )

        jar_id_query = upload_jar_and_get_id(config['jar_path'])
        query_job_id = run_flink_job(jar_id_query, program_args)
        print(f"{query_key} job started. Job ID: {query_job_id}")

        print("Waiting for 2 minutes before attempting to kill a random TaskManager...")
        time.sleep(5 * 60)
        subprocess.run([f"{FLINK_PATH}/bin/taskmanager.sh stop"], shell=True, check=True)
        time.sleep(2)
        subprocess.run([f"{FLINK_PATH}/bin/taskmanager.sh start"], shell=True, check=True)
        time.sleep(2 * 60)

        source_log_path = "/Users/frankli/Documents/flink_tool/flink-1.18.1/log/flink-frankli-standalonesession-0-crc-dot1x-nat-10-239-240-171.bu.edu.log"
        destination_dir = "/Volumes/ARION/flink-tool/temp/flink-log"
        destination_log_path = os.path.join(destination_dir, f"paral_{paralle}_interval_{5000}.log")
        os.makedirs(destination_dir, exist_ok=True)
        shutil.copy2(source_log_path, destination_log_path)

        stop_flink_job(query_job_id)
        print(f"{query_key} job stopped.")
