#!/bin/bash

TOPICS=("raw_data" "transformed_data")

KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"
KAFKA_BROKER="localhost:9092"

for topic in "${TOPICS[@]}"; do
    if ! $KAFKA_BIN --bootstrap-server $KAFKA_BROKER --list | grep -q $topic; then
        $KAFKA_BIN --bootstrap-server $KAFKA_BROKER --create --topic $topic --partitions 1 --replication-factor 1
        echo "Topic '$topic' created."
    else
        echo "Topic '$topic' already exists."
    fi
done