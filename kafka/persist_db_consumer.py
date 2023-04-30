import psycopg2
import json
from kafka import KafkaConsumer


conn = psycopg2.connect("postgres://rohan:rohan@localhost:5432/dbt")
cur = conn.cursor()

insert_query = "INSERT INTO stock_data(price, symbol, price_datetime, volume) VALUES(%s, %s, %s, %s)"

values = []
counter = 0
total = 0

consumer = KafkaConsumer("BINANCE_DB")

for message in consumer:
    data = message.value

    data = json.loads(data)
    values.append([
        data["p"],
        data["s"],
        data["datetime"],
        data["v"]
    ])
    counter += 1
    total += 1
    
    if counter == 10:
        cur.executemany(insert_query, values)
        conn.commit()
        counter = 0
        values = []
        
        print(total)