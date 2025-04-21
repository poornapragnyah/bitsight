from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import avg, date_format, col, desc

# create a SparkSession
# spark.sparkContext.addPyFile("path/to/postgresql-<version>.jar")
conf = SparkConf().setAppName("PostgresBatch") \
                  .set("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar")

# Create a SparkSession with the specified configuration properties

spark = SparkSession.builder.config(conf=conf).getOrCreate()


spark.conf


# define connection properties
url = "jdbc:postgresql://postgres:5432/dbt"  # Using service name instead of localhost
user = "rohan"
password = "rohan"

# define the query to fetch data

# read data from PostgreSQL database
df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("user", user) \
    .option("dbtable", "stock_data") \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# df.printSchema()

print("Time intervals for the following calculations.")
# Start timestamp
print("Start Timestamp")
start_timestamp = df.agg({"price_datetime": "min"})
start_timestamp.show()

# End timestamp
print("End Timestamp")
end_timestamp = df.agg({"price_datetime": "max"})
end_timestamp.show()

# Overall average price
print("Overall average price")
average = df.agg({"price": "avg"})
average.show()

# Calculating average price per minute
print("Calculating average price per minute")
df = df.withColumn("date", date_format(col("price_datetime"), "yyyy-MM-dd HH:mm"))
average_per_min = df.groupBy("date").mean("price")
average_per_min.show()


# Total purchase volume
print("Total Purchases")
volume_sum = df.agg({"volume": "sum"})
volume_sum.show()

# Total purchase volume per minute
print("Total purchase volume per minute")
sum_volume_per_min = df.groupBy("date").sum("volume").orderBy(desc("date"))
sum_volume_per_min.show()


spark.stop()