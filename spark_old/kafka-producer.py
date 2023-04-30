from kafka import KafkaProducer
import time
import csv
import json
from contextlib import suppress
from datetime import datetime


producer = KafkaProducer(bootstrap_servers='localhost:9092')


with open("../stock-market/ADANIPORTS.csv") as f:
    data = csv.DictReader(f)

    for row in data:

        for key, val in row.items():
            with suppress(TypeError, ValueError):
                row[key] = float(val)

        row["Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        json_string = json.dumps(row)
        print("sending...")
        producer.send('adaniports_input', json_string.encode())
        time.sleep(1)

# for _ in range(1):
#     print("Sending...")
#     producer.send('foobar', b'some,message,bytes')
    # time.sleep(1)

time.sleep(1)