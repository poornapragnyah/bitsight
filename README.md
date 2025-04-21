# Bitcoin Price Analysis with Spark and Kafka

This project implements both real-time streaming and batch processing of Bitcoin price data using Apache Spark, Apache Kafka, and PostgreSQL.

## Architecture

### Streaming Pipeline
1. Real-time Bitcoin price data from Finnhub WebSocket API
2. Kafka producer sends data to topics (`BINANCE_DB` and `SPARK_ANALYSIS`)
3. Database consumer writes raw data to PostgreSQL
4. Spark Streaming processes data in 10-second windows
5. Results consumer writes aggregated data to PostgreSQL

### Batch Pipeline
- Reads historical data from PostgreSQL
- Calculates various aggregates (average price, volume, etc.)
- Useful for historical analysis and reporting

## Prerequisites

1. Docker and Docker Compose
2. Python 3.x
3. Java 11 (Important: Spark 3.2.0 is not compatible with Java 17+)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
```

2. Install Java 11 (if not already installed):
```bash
# For Fedora/RHEL
sudo dnf install java-11-openjdk-devel

# For Ubuntu/Debian
sudo apt install openjdk-11-jdk

# Set Java 11 as default
sudo alternatives --config java  # Select Java 11 from the list
```

3. Create and activate Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Unix/macOS
# or
.\venv\Scripts\activate  # On Windows
```

4. Install Python dependencies:
```bash
pip install -r requirements.txt
```

5. Start the Docker containers:
```bash
docker compose up -d
```

## Running the Streaming Pipeline

1. Make the streaming script executable:
```bash
chmod +x streaming.sh
```

2. Run the streaming pipeline:
```bash
./streaming.sh
```

This will start:
- Finnhub WebSocket producer (`rt_stock_producer.py`)
- Spark Streaming job (`spark_streaming.py`)
- Database consumer (`persist_db_consumer.py`)
- Results consumer (`results_consumer.py`)

The streaming pipeline will:
- Collect real-time Bitcoin price data
- Store raw data in the `stock_data` table
- Process data in 10-second windows
- Store aggregated results in the `stock_aggregates` table

## Running the Batch Processing

1. Make the batch script executable:
```bash
chmod +x batch.sh
```

2. Run the batch processing:
```bash
./batch.sh
```

The batch processing will:
- Read all historical data from the `stock_data` table
- Calculate various aggregates:
  - Overall average price
  - Average price per minute
  - Total purchase volume
  - Purchase volume per minute
- Display results in the console

## Monitoring

### Check PostgreSQL Data

1. Raw data:
```bash
docker exec bitsight-postgres-1 psql -U rohan -d dbt -c "SELECT COUNT(*) FROM stock_data;"
docker exec bitsight-postgres-1 psql -U rohan -d dbt -c "SELECT * FROM stock_data ORDER BY price_datetime DESC LIMIT 5;"
```

2. Aggregated data:
```bash
docker exec bitsight-postgres-1 psql -U rohan -d dbt -c "SELECT * FROM stock_aggregates ORDER BY start_datetime DESC LIMIT 5;"
```

### Check Kafka Topics

1. List topics:
```bash
docker exec broker kafka-topics --list --bootstrap-server localhost:9092
```

2. Read messages from a topic:
```bash
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic BINANCE_DB --from-beginning
```

### Monitor Spark

1. Access Spark UI:
- Open http://localhost:8080 in your browser
- View active applications, jobs, and stages

## Troubleshooting

1. If Spark fails with Java version errors:
   - Ensure you're using Java 11 (`java -version`)
   - Switch to Java 11 if needed (`sudo alternatives --config java`)

2. If Kafka consumers aren't receiving messages:
   - Check if topics exist
   - Verify producer is running and sending data
   - Check Kafka broker logs

3. If data isn't appearing in PostgreSQL:
   - Verify database connection settings
   - Check consumer logs in `logs/` directory
   - Ensure consumers are running (`ps aux | grep consumer`)

## Project Structure

```
.
├── batch.sh              # Batch processing script
├── streaming.sh          # Streaming pipeline script
├── kafka/
│   ├── rt_stock_producer.py     # Finnhub WebSocket producer
│   ├── persist_db_consumer.py   # Database consumer
│   └── results_consumer.py      # Aggregates consumer
├── spark/
│   ├── spark_streaming.py       # Spark Streaming job
│   └── spark_batch.py          # Spark Batch job
└── postgres/
    └── init.sql                 # Database initialization
```

## Data Schema

### stock_data (Raw Data)
- id: SERIAL PRIMARY KEY
- price: DECIMAL
- symbol: VARCHAR(128)
- price_datetime: TIMESTAMP
- volume: DECIMAL

### stock_aggregates (Processed Data)
- id: SERIAL PRIMARY KEY
- start_datetime: TIMESTAMP
- end_datetime: TIMESTAMP
- average_price: DECIMAL
- total_volume: DECIMAL