import os

BROKER_ADDRESS = "127.0.0.1:9092"
KAFKA_PATH = os.environ.get("KAFKA_PATH")

CURRENT_DIRECTORY = os.getcwd()
FLINK_JM_IP = "localhost"
FLINK_JM_PORT = "8081"
TARGET_PATH = os.path.join(CURRENT_DIRECTORY, "target")

RATELIST = "250000_900000"

JAR_PATH = {
    'query1': os.path.join(TARGET_PATH, "Query1-jar-with-dependencies.jar"),
    'query3': os.path.join(TARGET_PATH, "Query3-jar-with-dependencies.jar"),
    'query5': os.path.join(TARGET_PATH, "Query5-jar-with-dependencies.jar"),
    'query8': os.path.join(TARGET_PATH, "Query8-jar-with-dependencies.jar"),

    'auction': os.path.join(TARGET_PATH, "KafkaSourceAuction-jar-with-dependencies.jar"),
    'person': os.path.join(TARGET_PATH, "KafkaSourcePerson-jar-with-dependencies.jar"),
    'bid': os.path.join(TARGET_PATH, "KafkaSourceBid-jar-with-dependencies.jarr"),
}

QUERIES = {
    '1': {'sources': ['bid'], 'topics': ['query1']},
    '3': {'sources': ['auction', 'person'], 'topics': ['query3_a', 'query3_p']},
    '5': {'sources': ['auction', 'person'], 'topics': ['query5_src', 'query5_sink']},
    '8': {'sources': ['auction', 'person'], 'topics': ['query8_a', 'query8_p']}
}
