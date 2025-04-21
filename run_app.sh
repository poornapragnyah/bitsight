#!/bin/bash

# Set Java options for Spark
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
export SPARK_HOME=/home/poorna/venvs/bitsight/lib64/python3.11/site-packages/pyspark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Set additional Java options
export _JAVA_OPTIONS="-XX:+UseParallelGC -XX:MaxDirectMemorySize=512m"
export SPARK_LOCAL_IP="127.0.0.1"
export SPARK_MASTER_HOST="127.0.0.1"

# Run the streaming script
./streaming.sh 