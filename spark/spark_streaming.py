import json
import logging
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window, sum as _sum, to_json, struct, expr
from pyspark.sql.types import (FloatType, StringType, StructField, StructType,
                               TimestampType)
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

# Set Spark configuration properties
conf = SparkConf().setAppName("KafkaStreamReader") \
                  .set("spark.sql.shuffle.partitions", "1") \
                  .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                  .set("spark.executor.memory", "1g") \
                  .set("spark.driver.memory", "1g") \
                  .set("spark.executor.cores", "1") \
                  .set("spark.executor.instances", "1") \
                  .set("spark.driver.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
                  .set("spark.executor.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar")

# Create a SparkSession with the specified configuration properties
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

logging.info("Spark session created successfully")

# Create a SparkSession
# spark = SparkSession.builder.appName("KafkaStreamReader").getOrCreate()


def sample(data):
    data = json.loads(data.decode('utf-8'))
    with open("sample.txt", "w+") as f:
        f.write(json.dumps(data))


# Define the schema for input data
schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("s", StringType(), True),
    StructField("p", FloatType(), True),
    StructField("v", FloatType(), True),
    StructField("c", StringType(), True),
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
    "startingOffsets": "earliest"
}

logging.info("Reading from Kafka topic SPARK_ANALYSIS")
# Read data from Kafka as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

# Parse the JSON data and apply watermark
logging.info("Parsing JSON data and applying watermark")
kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("datetime", "20 seconds")

# Perform windowed aggregation
logging.info("Performing windowed aggregation")
windowed_stream = parsed_df \
    .groupBy(window(col("datetime"), "10 seconds", "10 seconds")) \
    .agg(
        avg(col("p")).alias("average_price"),
        _sum(col("v")).alias("total_volume")
    )

# Write to console for debugging
logging.info("Starting console output stream")
console_write = windowed_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# Write to Kafka topic
logging.info("Starting Kafka output stream to SPARK_RESULTS topic")
kafka_write = windowed_stream \
    .select(
        expr("to_json(struct(window.start as start_datetime, window.end as end_datetime, average_price, total_volume)) as value")
    ) \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "SPARK_RESULTS") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .trigger(processingTime="10 seconds") \
    .start()

logging.info("Streaming jobs started. Waiting for termination...")

# Wait for termination
kafka_write.awaitTermination()
console_write.awaitTermination()

# stream_input = parsed_df.writeStream.format("console").start()
# stream_input.awaitTermination()


# Start streaming query and display results
# query = parsed_df.selectExpr("data.*").writeStream.format("console").start()
# query.awaitTermination()
