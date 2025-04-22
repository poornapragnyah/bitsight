from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
from datetime import datetime

# Configure logging
log_filename = f'logs/batch_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Set Spark configuration properties
conf = SparkConf().setAppName("PostgresBatch") \
                  .set("spark.sql.shuffle.partitions", "1") \
                  .set("spark.executor.memory", "512m") \
                  .set("spark.driver.memory", "512m") \
                  .set("spark.executor.cores", "1") \
                  .set("spark.executor.instances", "1") \
                  .set("spark.driver.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
                  .set("spark.executor.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
                  .set("spark.dynamicAllocation.enabled", "false") \
                  .set("spark.cores.max", "1") \
                  .set("spark.task.cpus", "1") \
                  .set("spark.sql.adaptive.enabled", "true") \
                  .set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Create a SparkSession with the specified configuration properties
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

logging.info("Spark session created successfully")

# Your batch processing code here
# For example:
# df = spark.read.format("jdbc") \
#     .option("url", "jdbc:postgresql://bitsight-postgres:5432/bitsight") \
#     .option("dbtable", "stock_data") \
#     .option("user", "poorna") \
#     .option("password", "poorna") \
#     .load()
#
# # Process the data
# result = df.groupBy("symbol").agg(
#     {"price": "avg", "volume": "sum"}
# )
#
# # Write results
# result.write.format("jdbc") \
#     .option("url", "jdbc:postgresql://bitsight-postgres:5432/bitsight") \
#     .option("dbtable", "stock_aggregates") \
#     .option("user", "poorna") \
#     .option("password", "poorna") \
#     .mode("append") \
#     .save()

spark.stop() 