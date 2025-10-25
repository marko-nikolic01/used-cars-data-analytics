import time
from kafka.admin import KafkaAdminClient, NewTopic
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPICS = [
    {"name": "raw_data", "partitions": 3, "replication_factor": 1},
    {"name": "transformed_data", "partitions": 3, "replication_factor": 1}
]
RETRY_DELAY = 5

def create_topics_forever():
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            existing_topics = admin_client.list_topics()
            new_topics = [
                NewTopic(
                    name=t["name"],
                    num_partitions=t["partitions"],
                    replication_factor=t["replication_factor"]
                )
                for t in TOPICS if t["name"] not in existing_topics
            ]
            if new_topics:
                admin_client.create_topics(new_topics=new_topics)
                print(f"Topics created: {[t.name for t in new_topics]}", flush=True)
            else:
                print(f"Topics already exist: {[t['name'] for t in TOPICS]}", flush=True)
            break
        except Exception as e:
            print(f"Kafka not available yet: {e}", flush=True)
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    create_topics_forever()
