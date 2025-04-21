#!/bin/bash

# Copy the Python script to the Spark container
docker cp spark/spark_batch.py spark-master:/opt/bitnami/spark/

# Run the Spark batch job inside the Spark container
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    /opt/bitnami/spark/spark_batch.py 