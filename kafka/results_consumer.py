import os
import logging
from datetime import datetime
import psycopg2
import json
from kafka import KafkaConsumer
import traceback

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
log_filename = f'logs/results_consumer_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="bitsight-postgres",
        database="bitsight",
        user="poorna",
        password="poorna",
        port="5432"
    )
    cur = conn.cursor()
    logging.info("Successfully connected to PostgreSQL")

    # Initialize Kafka consumer with correct broker hostname
    consumer = KafkaConsumer(
        "SPARK_RESULTS",
        bootstrap_servers=["broker:9092"],
        auto_offset_reset='earliest',
        group_id='results_consumer_group'
    )
    logging.info("Successfully connected to Kafka")

    # Process messages
    for message in consumer:
        try:
            data = json.loads(message.value)
            logging.info(f"Received message: {data}")

            # Insert into stock_aggregates table
            cur.execute("""
                INSERT INTO stock_aggregates (
                    start_datetime, end_datetime, average_price, total_volume
                ) 
                SELECT %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM stock_aggregates 
                    WHERE start_datetime = %s AND end_datetime = %s
                )
            """, [
                data["start_datetime"],
                data["end_datetime"],
                data["average_price"],
                data["total_volume"],
                data["start_datetime"],
                data["end_datetime"]
            ])
            
            conn.commit()
            logging.info("Successfully wrote metrics to stock_aggregates table")

        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            traceback.print_exc()

except Exception as e:
    logging.error(f"Error during results processing: {str(e)}")
    traceback.print_exc()

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    logging.info("Results processing finished")
