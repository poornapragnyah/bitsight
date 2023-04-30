# read from database and execute queries.

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

