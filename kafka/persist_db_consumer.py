import psycopg2
import json
from kafka import KafkaConsumer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/persist_db_consumer.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

try:
    conn = psycopg2.connect(
        host="bitsight-postgres",
        database="bitsight",
        user="poorna",
        password="poorna",
        port="5432"
    )
    cur = conn.cursor()
    logger.info("Successfully connected to PostgreSQL database")
except Exception as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")
    raise

insert_query = "INSERT INTO stock_data(price, symbol, price_datetime, volume) VALUES(%s, %s, %s, %s)"

values = []
counter = 0
total = 0

# Configure Kafka consumer with correct broker hostname
consumer = KafkaConsumer(
    "BINANCE_DB",
    bootstrap_servers=["broker:9092"],
    auto_offset_reset='earliest',
    group_id='db_consumer_group'
)

logger.info("Starting to consume messages from BINANCE_DB topic")

for message in consumer:
    try:
        data = json.loads(message.value)
        values.append([
            data["p"],
            data["s"],
            data["datetime"],
            data["v"]
        ])
        counter += 1
        total += 1
        
        if counter == 10:
            cur.executemany(insert_query, values)
            conn.commit()
            logger.info(f"Successfully inserted {counter} records. Total records: {total}")
            counter = 0
            values = []
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        continue