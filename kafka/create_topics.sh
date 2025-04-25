#!/bin/bash

# Create BINANCE_DB topic
docker exec broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic BINANCE_DB \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete \
    --config max.message.bytes=1048576

# Create SPARK_ANALYSIS topic
docker exec broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic SPARK_ANALYSIS \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete \
    --config max.message.bytes=1048576

# Create SPARK_RESULTS topic
docker exec broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic SPARK_RESULTS \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete \
    --config max.message.bytes=1048576

# List all topics to verify creation
echo "Listing all topics:"
docker exec broker kafka-topics --list --bootstrap-server localhost:9092 