#!/bin/bash

# Run the Spark batch job inside the Spark container
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/drivers/postgresql-42.6.0.jar \
    /opt/bitnami/spark/spark_batch.py 