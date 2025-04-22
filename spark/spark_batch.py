import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, min, max, count, stddev, expr

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgresBatch") \
    .config("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
    .getOrCreate()

try:
    # Read data from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://bitsight-postgres:5432/bitsight") \
        .option("dbtable", "stock_data") \
        .option("user", "poorna") \
        .option("password", "poorna") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    logging.info(f"Initial DataFrame count: {df.count()}")
    logging.info("DataFrame schema:")
    df.printSchema()
    logging.info("Sample data:")
    df.show(5)

    # Calculate comprehensive metrics
    metrics = df.agg(
        avg("price").alias("average_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        stddev("price").alias("price_stddev"),
        sum("volume").alias("total_volume"),
        avg("volume").alias("average_volume"),
        min("volume").alias("min_volume"),
        max("volume").alias("max_volume"),
        count("*").alias("total_records"),
        min("price_datetime").alias("start_datetime"),
        max("price_datetime").alias("end_datetime")
    ).collect()[0]

    # Log metrics
    logging.info(f"Calculated metrics:")
    logging.info(f"Price Statistics:")
    logging.info(f"  Average: ${metrics['average_price']:.2f}")
    logging.info(f"  Min: ${metrics['min_price']:.2f}")
    logging.info(f"  Max: ${metrics['max_price']:.2f}")
    logging.info(f"  StdDev: ${metrics['price_stddev']:.2f}")
    logging.info(f"Volume Statistics:")
    logging.info(f"  Total: {metrics['total_volume']:.4f} BTC")
    logging.info(f"  Average: {metrics['average_volume']:.4f} BTC")
    logging.info(f"  Min: {metrics['min_volume']:.4f} BTC")
    logging.info(f"  Max: {metrics['max_volume']:.4f} BTC")
    logging.info(f"Total Records: {metrics['total_records']}")
    logging.info(f"Data Range: {metrics['start_datetime']} to {metrics['end_datetime']}")

    # Write metrics to stock_aggregates table
    metrics_df = spark.createDataFrame([
        (
            metrics['average_price'],
            metrics['min_price'],
            metrics['max_price'],
            metrics['price_stddev'],
            metrics['total_volume'],
            metrics['average_volume'],
            metrics['min_volume'],
            metrics['max_volume'],
            metrics['total_records'],
            metrics['start_datetime'],
            metrics['end_datetime']
        )
    ], ["average_price", "min_price", "max_price", "price_stddev", "total_volume", "average_volume", "min_volume", "max_volume", "total_records", "start_datetime", "end_datetime"])
    metrics_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://bitsight-postgres:5432/bitsight") \
        .option("dbtable", "stock_aggregates_results") \
        .option("user", "poorna") \
        .option("password", "poorna") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logging.info("Successfully wrote metrics to stock_aggregates table")

except Exception as e:
    logging.error(f"Error during batch processing: {str(e)}")
    raise e

finally:
    logging.info("Batch processing finished")
    logging.info("Closing down clientserver connection")
    spark.stop()