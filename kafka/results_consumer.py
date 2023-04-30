import psycopg2
import json
from kafka import KafkaConsumer


conn = psycopg2.connect("postgres://rohan:rohan@localhost:5432/dbt")
cur = conn.cursor()

consumer = KafkaConsumer("SPARK_RESULTS")

for message in consumer:
    data = message.value
    data = json.loads(data)
    print(data)

    cur.execute(
        "INSERT INTO stock_aggregates(start_datetime, end_datetime, average_price, total_volume) VALUES(%s, %s, %s, %s)",
        [
            data["window"]["start"],
            data["window"]["end"],
            data["avg(data.p)"],
            data["sum(data.v)"]
        ]
    )
    conn.commit()

