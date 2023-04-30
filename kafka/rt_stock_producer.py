import json
import sys
import time
from datetime import datetime
from threading import Thread

import websocket

from kafka import KafkaProducer


# s: symbol, p: last price, t: unix time in ms, v: volume , c:conditions

producer = KafkaProducer(bootstrap_servers='localhost:9092')


def send_to_kafka(message):
    parsed = json.loads(message)
    if not parsed.get("data", None):
        return
    for entry in parsed["data"]:
        entry["datetime"] = str(datetime.fromtimestamp(entry["t"]/1000))

        del entry["t"]

        ser = json.dumps(entry)
        print("Sending...", end="")
        # persist data in db
        producer.send('BINANCE_DB', ser.encode("utf-8"))

        # send to spark for analysis
        producer.send('SPARK_ANALYSIS', ser.encode("utf-8"))

        print("Sent")
        time.sleep(1)


def on_message(ws, message: str) -> None:
    Thread(target=send_to_kafka, args=(message, )).start()


def on_error(ws, error) -> None:
    print(error)


def on_close(ws) -> None:
    print("### closed ###")


def on_open(ws) -> None:
    # Subscribe to BITCOIN price updates.
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')


if __name__ == "__main__":
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=ch71859r01qhmmunvi50ch71859r01qhmmunvi5g",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
