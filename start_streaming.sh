#!/bin/bash

# Exit on error
set -e

# Make sure we're in the right directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Create logs directory if it doesn't exist
mkdir -p logs

# Function to handle errors
handle_error() {
    echo "Error occurred in $1"
    echo "Check logs/$1.log for details"
    exit 1
}

# Check if required files exist
if [ ! -f "spark/spark_streaming.py" ]; then
    echo "Error: spark_streaming.py not found in spark directory"
    exit 1
fi

if [ ! -f "kafka/rt_stock_producer.py" ] || [ ! -f "kafka/results_consumer.py" ] || [ ! -f "kafka/persist_db_consumer.py" ]; then
    echo "Error: Kafka scripts not found in kafka directory"
    exit 1
fi

# Copy all necessary files to the Spark container
echo "Copying files to Spark container..."
docker cp spark/spark_streaming.py spark-master:/opt/bitnami/spark/
docker cp kafka/results_consumer.py spark-master:/opt/bitnami/spark/
docker cp kafka/persist_db_consumer.py spark-master:/opt/bitnami/spark/
docker cp kafka/rt_stock_producer.py spark-master:/opt/bitnami/spark/

# Start Spark streaming job
echo "Starting Spark streaming job..."
docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    /opt/bitnami/spark/spark_streaming.py > logs/spark_streaming.log 2>&1

# Start Kafka producer
echo "Starting Kafka producer..."
docker exec -d spark-master python3 /opt/bitnami/spark/rt_stock_producer.py > logs/rt_stock_producer.log 2>&1

# Start results consumer
echo "Starting results consumer..."
docker exec -d spark-master python3 /opt/bitnami/spark/results_consumer.py > logs/results_consumer.log 2>&1

# Start database consumer
echo "Starting database consumer..."
docker exec -d spark-master python3 /opt/bitnami/spark/persist_db_consumer.py > logs/persist_db_consumer.log 2>&1

# Function to clean up processes
cleanup() {
    echo "Stopping all processes..."
    docker exec spark-master pkill -f "spark-submit"
    docker exec spark-master pkill -f "python3.*rt_stock_producer.py"
    docker exec spark-master pkill -f "python3.*results_consumer.py"
    docker exec spark-master pkill -f "python3.*persist_db_consumer.py"
    exit 0
}

# Set up trap for Ctrl+C
trap cleanup SIGINT

echo "All streaming components started. Press Ctrl+C to stop."
echo "Logs are being written to the logs/ directory:"
echo "- spark_streaming.log"
echo "- rt_stock_producer.log"
echo "- results_consumer.log"
echo "- persist_db_consumer.log"

# Wait for all processes
wait 