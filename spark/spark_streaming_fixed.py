import json
import logging
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window, sum as _sum, to_json, struct, expr, min, max, count, current_timestamp, stddev, coalesce, to_timestamp
from pyspark.sql.types import (FloatType, StringType, StructField, StructType, TimestampType)
from kafka import KafkaProducer

# Configure logging
log_filename = f'logs/streaming_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Performance metrics
class PerformanceMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.total_records = 0
        self.metrics = {
            'timestamp': [],
            'records_processed': [],
            'processing_time': [],
            'avg_price': [],
            'total_volume': []
        }
        self.last_batch_time = self.start_time
        self.window_size = 10  # 10-second window

    def update_metrics(self, records, processing_time, avg_price, total_volume):
        current_time = time.time()
        batch_time = current_time - self.last_batch_time
        self.last_batch_time = current_time
        
        self.total_records += records
        self.metrics['timestamp'].append(datetime.now())
        self.metrics['records_processed'].append(records)
        self.metrics['processing_time'].append(batch_time)
        self.metrics['avg_price'].append(avg_price)
        self.metrics['total_volume'].append(total_volume)
        
        # Calculate and log metrics
        avg_processing_time = sum(self.metrics['processing_time']) / len(self.metrics['processing_time'])
        records_per_second = records / batch_time  # Actual processing rate for this window
        
        print("\n" + "="*80)
        print("STREAMING PERFORMANCE METRICS:")
        print(f"Records in Window: {records}")
        print(f"Processing Time: {batch_time:.2f} seconds")
        print(f"Processing Rate: {records_per_second:.2f} records/second")
        print(f"Average Price: ${avg_price:.2f}")
        print(f"Total Volume: {total_volume:.4f}")
        print("="*80 + "\n")

metrics = PerformanceMetrics()

# Set Spark configuration properties
conf = SparkConf().setAppName("KafkaStreamReader") \
                  .set("spark.sql.shuffle.partitions", "1") \
                  .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                  .set("spark.executor.memory", "512m") \
                  .set("spark.driver.memory", "512m") \
                  .set("spark.executor.cores", "1") \
                  .set("spark.executor.instances", "1") \
                  .set("spark.driver.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
                  .set("spark.executor.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
                  .set("spark.dynamicAllocation.enabled", "false") \
                  .set("spark.cores.max", "1") \
                  .set("spark.task.cpus", "1") \
                  .set("spark.streaming.concurrentJobs", "1") \
                  .set("spark.streaming.kafka.maxRatePerPartition", "100")

# Create a SparkSession with the specified configuration properties
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

logging.info("Spark session created successfully")

def sample(data):
    data = json.loads(data.decode('utf-8'))
    with open("sample.txt", "w+") as f:
        f.write(json.dumps(data))

# Define the schema for input data
schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("s", StringType(), True),
    StructField("p", FloatType(), True),  # price
    StructField("v", FloatType(), True),  # volume
    StructField("c", StringType(), True)
])

logging.info("Defined input schema")

# Kafka producer for writing results
producer = KafkaProducer(
    bootstrap_servers=["broker:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define Kafka source configuration
kafka_params = {
    "kafka.bootstrap.servers": "broker:9092",
    "subscribe": "SPARK_ANALYSIS",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 10000
}

logging.info("Reading from Kafka topic SPARK_ANALYSIS")
logging.info(f"Kafka parameters: {kafka_params}")

# Read data from Kafka as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

# Parse the JSON data and apply watermark
logging.info("Parsing JSON data and applying watermark")
kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
logging.info("Kafka DataFrame schema:")
kafka_df.printSchema()

# Debug the raw data
kafka_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("datetime", to_timestamp(col("datetime"))) \
    .withWatermark("datetime", "20 seconds")

# Log the schema
logging.info("Parsed DataFrame schema:")
parsed_df.printSchema()

# Debug the parsed data
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Perform windowed aggregation
logging.info("Performing windowed aggregation")
windowed_stream = parsed_df \
    .groupBy(window(col("datetime"), "10 seconds", "10 seconds")) \
    .agg(
        avg(col("p")).alias("average_price"),
        _sum(col("v")).alias("total_volume"),
        count("*").alias("total_records"),
        min(col("datetime")).alias("start_datetime"),
        max(col("datetime")).alias("end_datetime")
    )

# Log the windowed stream schema
logging.info("Windowed stream schema:")
windowed_stream.printSchema()

# Write to Kafka topic
logging.info("Starting Kafka output stream to SPARK_RESULTS topic")
kafka_write = windowed_stream \
    .select(
        expr("""
            to_json(struct(
                window.start as start_datetime,
                window.end as end_datetime,
                COALESCE(average_price, 0.0) as average_price,
                COALESCE(total_volume, 0.0) as total_volume,
                total_records
            )) as value
        """)
    ) \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "SPARK_RESULTS") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .trigger(processingTime="10 seconds") \
    .start()

def process_batch(batch_df, batch_id):
    # Print the schema and data for debugging
    logging.info("Batch Schema:")
    batch_df.printSchema()
    logging.info("Batch Data:")
    batch_df.show(truncate=False)
    
    # Calculate metrics
    record_count = batch_df.select("total_records").collect()[0][0]
    batch_time = time.time() - metrics.start_time
    
    # Get metrics with NULL handling
    metrics_row = batch_df.collect()[0]
    avg_price = metrics_row["average_price"] if metrics_row["average_price"] is not None else 0.0
    total_volume = metrics_row["total_volume"] if metrics_row["total_volume"] is not None else 0.0
    
    # Update metrics
    metrics.update_metrics(record_count, batch_time, avg_price, total_volume)

# Write to console for debugging
logging.info("Starting console output stream")
console_write = windowed_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(process_batch) \
    .start()

logging.info("Streaming jobs started. Waiting for termination...")

# Wait for termination
kafka_write.awaitTermination()
console_write.awaitTermination()

# Clean up resources
spark.stop()
logging.info("Spark session stopped") 