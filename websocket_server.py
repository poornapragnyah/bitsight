import asyncio
import websockets
import json
import psycopg2
import time
import logging
import os
from datetime import datetime, timedelta

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging
log_file = f'logs/websocket_server_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"Starting WebSocket server. Log file: {log_file}")

# Database connection
try:
    conn = psycopg2.connect(
        host="localhost",
        database="bitsight",
        user="poorna",
        password="poorna",
        port="5432"
    )
    logger.info("Successfully connected to PostgreSQL database")
except Exception as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")
    raise

async def get_latest_data():
    cur = conn.cursor()
    try:
        # Get latest price and volume with a 10-second window
        cur.execute("""
            WITH window_data AS (
                SELECT 
                    date_trunc('second', price_datetime) as window_start,
                    date_trunc('second', price_datetime) + interval '10 seconds' as window_end,
                    AVG(price) as avg_price,
                    SUM(volume) as total_volume
                FROM stock_data 
                WHERE price_datetime > NOW() - INTERVAL '20 seconds'
                GROUP BY date_trunc('second', price_datetime)
                ORDER BY window_start DESC
                LIMIT 1
            )
            SELECT 
                window_start,
                window_end,
                avg_price,
                total_volume
            FROM window_data
        """)
        latest = cur.fetchone()
        
        if latest and latest[2] is not None:  # Check if we have data
            window_start, window_end, avg_price, total_volume = latest
            logger.info(f"Latest data - Price: {avg_price}, Volume: {total_volume}, Window: {window_start} - {window_end}")
            
            return {
                "price": float(avg_price),
                "volume": float(total_volume),
                "window": {
                    "start": window_start.strftime("%H:%M:%S"),
                    "end": window_end.strftime("%H:%M:%S")
                }
            }
        else:
            logger.warning("No recent data found in stock_data table")
    except Exception as e:
        logger.error(f"Error getting latest data: {e}")
    finally:
        cur.close()
    return None

async def get_batch_metrics():
    cur = conn.cursor()
    try:
        # Get the latest batch metrics
        cur.execute("""
            SELECT 
                start_datetime,
                end_datetime,
                average_price,
                min_price,
                max_price,
                price_stddev,
                total_volume,
                average_volume,
                min_volume,
                max_volume,
                total_records
            FROM stock_aggregates_new
            ORDER BY id DESC
            LIMIT 1
        """)
        metrics = cur.fetchone()
        
        if metrics:
            start_time, end_time, avg_price, min_price, max_price, price_stddev, total_vol, avg_vol, min_vol, max_vol, records = metrics
            logger.info(f"Batch metrics - Avg Price: {avg_price}, Total Vol: {total_vol}, Records: {records}")
            return {
                "start_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "average_price": float(avg_price),
                "min_price": float(min_price),
                "max_price": float(max_price),
                "price_stddev": float(price_stddev),
                "total_volume": float(total_vol),
                "average_volume": float(avg_vol),
                "min_volume": float(min_vol),
                "max_volume": float(max_vol),
                "total_records": records
            }
        else:
            logger.warning("No data found in stock_aggregates_new table")
    except Exception as e:
        logger.error(f"Error getting batch metrics: {e}")
    finally:
        cur.close()
    return None

async def get_streaming_metrics():
    cur = conn.cursor()
    try:
        # Get streaming processing metrics for the last 5 minutes
        cur.execute("""
            WITH recent_streams AS (
                SELECT 
                    EXTRACT(EPOCH FROM (end_datetime - start_datetime)) * 1000 as processing_time_ms
                FROM stock_aggregates
                WHERE end_datetime > NOW() - INTERVAL '5 minutes'
            )
            SELECT 
                COUNT(*) as total_records,
                AVG(processing_time_ms) as avg_processing_time
            FROM recent_streams
        """)
        metrics = cur.fetchone()
        
        if metrics and metrics[0] is not None:
            total_records, avg_time = metrics
            logger.info(f"Streaming metrics - Total Records: {total_records}, Avg Time: {avg_time}ms")
            return {
                "total_records": total_records,
                "avg_processing_time": float(avg_time) if avg_time else 0
            }
        else:
            logger.warning("No recent data found in stock_aggregates table")
    except Exception as e:
        logger.error(f"Error getting streaming metrics: {e}")
    finally:
        cur.close()
    return None

async def handler(websocket):
    logger.info(f"New WebSocket connection from {websocket.remote_address}")
    while True:
        try:
            # Get latest data
            latest_data = await get_latest_data()
            if latest_data:
                await websocket.send(json.dumps({
                    "type": "realtime",
                    "data": latest_data
                }))
            
            # Get batch metrics
            batch_metrics = await get_batch_metrics()
            if batch_metrics:
                await websocket.send(json.dumps({
                    "type": "batch",
                    "data": batch_metrics
                }))
            
            # Get streaming metrics
            stream_metrics = await get_streaming_metrics()
            if stream_metrics:
                await websocket.send(json.dumps({
                    "type": "streaming",
                    "data": stream_metrics
                }))
            
            await asyncio.sleep(1)  # Update every second
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"WebSocket connection closed for {websocket.remote_address}")
            break
        except Exception as e:
            logger.error(f"Error in handler: {e}")
            await asyncio.sleep(1)

async def main():
    async with websockets.serve(handler, "localhost", 8081):
        logger.info("WebSocket server started on ws://localhost:8081")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main()) 