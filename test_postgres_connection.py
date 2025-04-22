from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Connection Test") \
    .config("spark.jars", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/drivers/postgresql-42.6.0.jar") \
    .getOrCreate()

# PostgreSQL connection properties
db_properties = {
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://bitsight-postgres:5432/bitsight",
    "user": "poorna",
    "password": "poorna"
}

try:
    # Try to read from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", db_properties["url"]) \
        .option("driver", db_properties["driver"]) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("dbtable", "(SELECT 1 as test) as test_table") \
        .load()
    
    # Show the result
    df.show()
    print("Successfully connected to PostgreSQL!")
    
except Exception as e:
    print(f"Error connecting to PostgreSQL: {str(e)}")

# Stop SparkSession
spark.stop() 