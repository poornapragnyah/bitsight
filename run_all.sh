#!/bin/bash

# Exit on error
set -e

# Make sure we're in the right directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Kill any existing processes on port 8081
echo "Checking for existing processes on port 8081..."
lsof -ti:8081 | xargs -r kill -9

# Check if required files exist
if [ ! -f "drivers/postgresql-42.6.0.jar" ]; then
    echo "Error: PostgreSQL driver not found in drivers directory"
    exit 1
fi

if [ ! -f "spark/spark_streaming.py" ] || [ ! -f "spark/spark_batch.py" ]; then
    echo "Error: Spark scripts not found in spark directory"
    exit 1
fi

# Function to clean up processes on script exit
cleanup() {
    echo "Stopping all processes..."
    # Kill all background processes
    pkill -f "python kafka/rt_stock_producer.py"
    pkill -f "python kafka/persist_db_consumer.py"
    pkill -f "python websocket_server.py"
    pkill -f "python kafka/results_consumer.py"
    # Stop Docker containers
    docker compose down
    exit 0
}

# Set up trap to catch Ctrl+C and cleanup
trap cleanup SIGINT SIGTERM

# Start all services
echo "Starting all services..."
docker compose up -d

sleep 5
# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until docker exec bitsight-postgres pg_isready -U poorna; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done

echo "PostgreSQL is up and ready!"

# Wait for other services to be ready
echo "Waiting for other services to be ready..."
sleep 10

# Check if Docker containers are running
if [ "$(docker ps -a | grep 'postgres' | wc -l)" -eq 0 ]; then
    echo "Error: PostgreSQL container is not running"
    cleanup
    exit 1
fi
if [ "$(docker ps -a | grep 'spark' | wc -l)" -eq 0 ]; then
    echo "Error: Spark container is not running"
    cleanup
    exit 1
fi

# Start Kafka producer in the background
echo "Starting Kafka producer..."
python kafka/rt_stock_producer.py &
PRODUCER_PID=$!

# Start Kafka consumer in the background
echo "Starting Kafka consumer..."
python kafka/persist_db_consumer.py &
CONSUMER_PID=$!

# Start Kafka results consumer in the background
echo "Starting Kafka results consumer..."
python kafka/results_consumer.py &
RESULTS_CONSUMER_PID=$!

# Copy Spark scripts to the Spark container
echo "Copying Spark scripts to container..."
docker cp spark/spark_streaming.py spark-master:/opt/bitnami/spark/
docker cp spark/spark_batch.py spark-master:/opt/bitnami/spark/

# Create necessary directories in containers
echo "Creating necessary directories..."
docker exec spark-master mkdir -p /opt/bitnami/spark/drivers
docker exec spark-master mkdir -p /tmp/checkpoint
docker exec bitsight-spark-worker-1 mkdir -p /opt/bitnami/spark/drivers
docker exec bitsight-spark-worker-1 mkdir -p /tmp/checkpoint

# Copy PostgreSQL driver to Spark containers
echo "Copying PostgreSQL driver..."
docker cp drivers/postgresql-42.6.0.jar spark-master:/opt/bitnami/spark/drivers/
docker cp drivers/postgresql-42.6.0.jar bitsight-spark-worker-1:/opt/bitnami/spark/drivers/

echo "Setting permissions for PostgreSQL driver..."
docker exec --user root spark-master chmod 644 /opt/bitnami/spark/drivers/postgresql-42.6.0.jar
docker exec --user root bitsight-spark-worker-1 chmod 644 /opt/bitnami/spark/drivers/postgresql-42.6.0.jar

# Start the streaming job in the background
echo "Starting Spark streaming job..."
docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    --driver-class-path /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    --conf "spark.executor.extraClassPath=/opt/bitnami/spark/drivers/postgresql-42.6.0.jar" \
    /opt/bitnami/spark/spark_streaming.py

# Start the WebSocket server in the background
echo "Starting WebSocket server..."
python websocket_server.py &
WEBSOCKET_PID=$!

# Wait a bit for the streaming job to start
sleep 10

# Run the batch job
# echo "Running batch job..."
# docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
#     --master spark://spark-master:7077 \
#     --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
#     --driver-class-path /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
#     --conf "spark.executor.extraClassPath=/opt/bitnami/spark/drivers/postgresql-42.6.0.jar" \
#     /opt/bitnami/spark/spark_batch.py

# echo "All components are running!"
# echo "You can access the dashboard at http://localhost:8080"
# echo "Press Ctrl+C to stop all services"

# Keep the script running and check for process status
while true; do
    # Check if any of our background processes have died
    if ! kill -0 $PRODUCER_PID 2>/dev/null || ! kill -0 $CONSUMER_PID 2>/dev/null || ! kill -0 $WEBSOCKET_PID 2>/dev/null; then
        echo "One or more processes have stopped unexpectedly"
        cleanup
        exit 1
    fi
    sleep 1
done 