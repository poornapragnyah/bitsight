#!/bin/bash

# Exit on error
set -e

# Make sure we're in the right directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Create logs directory if it doesn't exist
mkdir -p logs

# Function to log messages with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to handle errors
handle_error() {
    log "Error occurred in $1"
    log "Check logs/$1.log for details"
    exit 1
}

# Check if required files exist
if [ ! -f "spark/spark_streaming.py" ]; then
    log "Error: spark_streaming.py not found in spark directory"
    exit 1
fi

if [ ! -f "kafka/rt_stock_producer.py" ] || [ ! -f "kafka/results_consumer.py" ] || [ ! -f "kafka/persist_db_consumer.py" ]; then
    log "Error: Kafka scripts not found in kafka directory"
    exit 1
fi

# Install required Python packages in the Spark container
log "Installing required Python packages..."
docker exec spark-master pip3 install kafka-python websocket-client psycopg2-binary > /dev/null 2>&1

# Copy all necessary files to the Spark container
log "Copying files to Spark container..."
docker cp spark/spark_streaming.py spark-master:/opt/bitnami/spark/
docker cp kafka/results_consumer.py spark-master:/opt/bitnami/spark/
docker cp kafka/persist_db_consumer.py spark-master:/opt/bitnami/spark/
docker cp kafka/rt_stock_producer.py spark-master:/opt/bitnami/spark/

# Function to start a component with proper logging
start_component() {
    local name=$1
    local cmd=$2
    local log_file="logs/${name}.log"
    
    log "Starting $name..."
    # Create a new log file with timestamp
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting $name" > "$log_file"
    
    # Start the component and append output to log file
    docker exec -d spark-master bash -c "$cmd >> /opt/bitnami/spark/${name}.log 2>&1"
    
    # Copy logs from container to host periodically (silently)
    (while true; do
        docker cp spark-master:/opt/bitnami/spark/${name}.log "$log_file" > /dev/null 2>&1
        sleep 5
    done) &
    
    # Store the PID of the log copying process
    eval "${name}_LOG_PID=$!"
}

# Start Spark streaming job
start_component "spark_streaming" "/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    /opt/bitnami/spark/spark_streaming.py"

# Start Kafka producer
start_component "rt_stock_producer" "python3 /opt/bitnami/spark/rt_stock_producer.py"

# Start results consumer
start_component "results_consumer" "python3 /opt/bitnami/spark/results_consumer.py"

# Start database consumer
start_component "persist_db_consumer" "python3 /opt/bitnami/spark/persist_db_consumer.py"

# Function to clean up processes
cleanup() {
    log "Stopping all processes..."
    
    # Stop the components
    docker exec spark-master pkill -f "spark-submit"
    docker exec spark-master pkill -f "python3.*rt_stock_producer.py"
    docker exec spark-master pkill -f "python3.*results_consumer.py"
    docker exec spark-master pkill -f "python3.*persist_db_consumer.py"
    
    # Stop the log copying processes
    kill $spark_streaming_LOG_PID
    kill $rt_stock_producer_LOG_PID
    kill $results_consumer_LOG_PID
    kill $persist_db_consumer_LOG_PID
    
    log "All processes stopped"
    exit 0
}

# Set up trap for Ctrl+C
trap cleanup SIGINT

log "All streaming components started. Press Ctrl+C to stop."
log "Logs are being written to the logs/ directory:"
log "- spark_streaming.log"
log "- rt_stock_producer.log"
log "- results_consumer.log"
log "- persist_db_consumer.log"
log ""
log "To monitor logs, use:"
log "tail -f logs/*.log"
log ""

# Wait for all processes
wait 