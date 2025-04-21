-- Create users
CREATE USER poorna WITH PASSWORD 'poorna';
CREATE USER sanya WITH PASSWORD 'sanya';

-- Create database
CREATE DATABASE dbt;

-- Grant privileges to users
GRANT ALL PRIVILEGES ON DATABASE dbt TO poorna;
GRANT ALL PRIVILEGES ON DATABASE dbt TO sanya;

-- Connect to the database
\c dbt

-- Create tables
CREATE TABLE IF NOT EXISTS stock_data (
	id SERIAL PRIMARY KEY,
	price DECIMAL,
	symbol VARCHAR(128),
	price_datetime TIMESTAMP WITHOUT TIME ZONE,
	volume DECIMAL
);

CREATE TABLE IF NOT EXISTS stock_aggregates (
	id SERIAL PRIMARY KEY,
	start_datetime TIMESTAMP WITHOUT TIME ZONE,
	end_datetime TIMESTAMP WITHOUT TIME ZONE,
	average_price DECIMAL,
	total_volume DECIMAL
);

-- Grant table privileges to users
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO poorna;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sanya;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO poorna;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sanya;