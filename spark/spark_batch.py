import os
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, min, max, count, stddev, to_timestamp, date_format

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

logger = logging.getLogger(__name__)

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

    def update_metrics(self, records, processing_time, avg_price, total_volume):
        self.total_records += records
        self.metrics['timestamp'].append(datetime.now())
        self.metrics['records_processed'].append(records)
        self.metrics['processing_time'].append(processing_time)
        self.metrics['avg_price'].append(avg_price)
        self.metrics['total_volume'].append(total_volume)
        
        # Calculate and log metrics using Python's built-in sum
        avg_processing_time = sum(self.metrics['processing_time']) / len(self.metrics['processing_time'])
        records_per_second = records / processing_time if processing_time > 0 else 0
        
        logger.info("\n" + "="*80)
        logger.info("BATCH PROCESSING METRICS:")
        logger.info(f"Total Records Processed: {self.total_records}")
        logger.info(f"Processing Time: {processing_time:.2f} seconds")
        logger.info(f"Processing Rate: {records_per_second:.2f} records/second")
        logger.info(f"Average Price: ${avg_price:.2f}")
        logger.info(f"Total Volume: {total_volume:.4f}")
        logger.info("="*80 + "\n")

def main():
    try:
        metrics = PerformanceMetrics()
        batch_start_time = time.time()
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("StockDataBatchProcessing") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.driver.cores", "1") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.log", "spark_batch.log") \
            .config("spark.executor.log", "spark_batch.log") \
            .config("spark.dynamicAllocation.enabled", "false") \
            .config("spark.cores.max", "1") \
            .config("spark.task.cpus", "1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info("Starting batch processing of historical data")
        
        # Read data from PostgreSQL
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/bitsight") \
            .option("dbtable", "stock_data") \
            .option("user", "poorna") \
            .option("password", "poorna") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # Log initial data
        record_count = df.count()
        logger.info(f"Found {record_count} records in stock_data table")
        logger.info("DataFrame Schema:")
        df.printSchema()
        logger.info("Sample data:")
        df.show(5)
        
        # Calculate metrics
        metrics_df = df.select(
            min("price_datetime").alias("start_datetime"),
            max("price_datetime").alias("end_datetime"),
            avg("price").alias("average_price"),
            spark_sum("volume").alias("total_volume")
        )
        
        # Log the calculated metrics
        logger.info("Calculated metrics:")
        metrics_df.show()
        
        # Write metrics to PostgreSQL
        metrics_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/bitsight") \
            .option("dbtable", "stock_aggregates_results") \
            .option("user", "poorna") \
            .option("password", "poorna") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        # Calculate and log performance metrics
        batch_end_time = time.time()
        processing_time = batch_end_time - batch_start_time
        
        # Get the metrics values
        metrics_row = metrics_df.collect()[0]
        avg_price = metrics_row["average_price"] if metrics_row["average_price"] is not None else 0.0
        total_volume = metrics_row["total_volume"] if metrics_row["total_volume"] is not None else 0.0
        
        # Update metrics
        metrics.update_metrics(record_count, processing_time, avg_price, total_volume)
        
        logger.info("Successfully wrote metrics to stock_aggregates_results table")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()