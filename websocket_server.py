import asyncio
import websockets
import json
import psycopg2
import time
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
try:
    conn = psycopg2.connect(
        host="localhost",
        database="dbt",
        user="rohan",
        password="rohan",
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
            SELECT 
                AVG(price) as avg_price,
                SUM(volume) as total_volume,
                MAX(price_datetime) as window_end,
                MIN(price_datetime) as window_start
            FROM stock_data 
            WHERE price_datetime > NOW() - INTERVAL '10 seconds'
        """)
        latest = cur.fetchone()
        
        if latest and latest[0] is not None:  # Check if we have data
            avg_price, total_volume, window_end, window_start = latest
            logger.info(f"Latest data - Price: {avg_price}, Volume: {total_volume}")
            
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
        # Get batch processing metrics for the last hour
        cur.execute("""
            WITH recent_batches AS (
                SELECT 
                    start_datetime,
                    end_datetime,
                    average_price,
                    total_volume,
                    EXTRACT(EPOCH FROM (end_datetime - start_datetime)) * 1000 as processing_time_ms
                FROM stock_aggregates
                WHERE end_datetime > NOW() - INTERVAL '1 hour'
            )
            SELECT 
                AVG(average_price) as avg_price,
                SUM(total_volume) as total_vol,
                MIN(start_datetime) as start_time,
                MAX(end_datetime) as end_time,
                COUNT(*) as records,
                AVG(processing_time_ms) as avg_processing_time
            FROM recent_batches
        """)
        metrics = cur.fetchone()
        
        if metrics and metrics[0] is not None:
            avg_price, total_vol, start_time, end_time, records, processing_time = metrics
            logger.info(f"Batch metrics - Avg Price: {avg_price}, Total Vol: {total_vol}, Records: {records}, Processing Time: {processing_time}ms")
            return {
                "average_price": float(avg_price),
                "total_volume": float(total_vol),
                "start_datetime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_datetime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "records_processed": records,
                "processing_time": float(processing_time) if processing_time else 0
            }
        else:
            logger.warning("No recent data found in stock_aggregates table")
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