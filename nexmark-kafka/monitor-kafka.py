import requests
import threading
import time
from kafka import KafkaConsumer

# Kafka setup
topic_name = 'query8_a'
kafka_broker = '127.0.0.1:9092'
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_broker],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id="auction-kafka-group"
)

flink_job_id = '940b9e9d78cdfcb42029bfe15af78d9b'
flink_rest_api = 'http://localhost:8081'
job_details_url = f"{flink_rest_api}/jobs/{flink_job_id}"

output_file = open('kafka_metrics.csv', 'w')
output_file.write('Timestamp,Messages Per Second,Current Offsets,Unread Messages\n')


def monitor_kafka():
    print("Monitoring Kafka...")
    message_count = 0
    total_messages = 0
    start_time = time.time()
    try:
        for message in consumer:
            message_count += 1
            total_messages += 1
            elapsed_time = time.time() - start_time

            if elapsed_time >= 2:
                messages_per_second = (message_count / elapsed_time)
                print(f"Messages every 2 second: {messages_per_second}")
                current_offsets = {tp: consumer.position(tp) for tp in consumer.assignment()}
                end_offsets = consumer.end_offsets(consumer.assignment())
                unread = {tp: end_offsets[tp] - current_offsets[tp] for tp in consumer.assignment()}
                print(f"Current offset: {current_offsets}, Unread: {unread}, Total: {total_messages}")

                output_file.write(f"{time.strftime('%H:%M:%S')},{messages_per_second},{current_offsets},{unread}\n")
                output_file.flush()

                message_count = 0
                start_time = time.time()

            consumer.commit()
    except Exception as e:
        print(f"Kafka error: {e}")


def monitor_flink():
    print("Monitoring Flink job status...")
    while True:
        try:
            response = requests.get(job_details_url)
            if response.status_code == 200:
                job_data = response.json()
                print("Job Status:", job_data.get('state'))
            else:
                print("Failed to fetch job details", response.status_code)
        except Exception as e:
            print(f"Flink API error: {e}")
        time.sleep(10)


if __name__ == "__main__":
    thread_kafka = threading.Thread(target=monitor_kafka)
    # thread_flink = threading.Thread(target=monitor_flink)

    thread_kafka.start()
    # thread_flink.start()

    thread_kafka.join()
    # thread_flink.join()
