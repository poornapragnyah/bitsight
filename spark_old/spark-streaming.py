from pyspark.sql import SparkSession
from pyspark import SparkConf
from kafka import KafkaConsumer
import json
from pyspark.sql import Row
import pandas as pd
from datetime import datetime

# Set Spark configuration properties
conf = SparkConf().setAppName("KafkaStreamReader")

# Create a SparkSession with the specified configuration properties
spark = SparkSession.builder.config(conf=conf).getOrCreate()

consumer = KafkaConsumer('foobar', value_deserializer=lambda x: json.loads(x.decode('utf-8')))


def default_dict():
    return {
        "Date": [],
        "Symbol": [],
        "Series": [],
        "Prev Close": [],
        "Open": [],
        "High": [],
        "Low": [],
        "Last": [],
        "Close": [],
        "VWAP": [],
        "Volume": [],
        "Turnover": [],
        "Trades": [],
        "Deliverable Volume": [],
        "%Deliverble": []
    }

window_year_month = None
data_per_month = default_dict()

def analyse(df):
    df = spark.createDataFrame(df)
    print(df.info())


for message in consumer:
    data = message.value

    stock_date = datetime.strptime(data["Date"], "%Y-%m-%d").date()

    if window_year_month is None:
        window_year_month = stock_date

    if window_year_month.year == stock_date.year and window_year_month.month == stock_date.month:
        # do analysis
 
        for k, v in data.items():
            data_per_month[k].append(v)

    else:
        df = pd.DataFrame(data_per_month)
        analyse(df)

        window_year_month = stock_date
        data_per_month = default_dict()



    


