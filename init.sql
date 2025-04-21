-- Create database if it doesn't exist
CREATE DATABASE dbt;

-- Connect to the database
\c dbt;

-- Create stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    price_datetime TIMESTAMP NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    volume DECIMAL(20,8) NOT NULL
);

-- Create stock_aggregates table
CREATE TABLE IF NOT EXISTS stock_aggregates (
    id SERIAL PRIMARY KEY,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    average_price DECIMAL(10,2) NOT NULL,
    total_volume DECIMAL(20,8) NOT NULL
);

-- Create user and grant permissions
CREATE USER rohan WITH PASSWORD 'rohan';
GRANT ALL PRIVILEGES ON DATABASE dbt TO rohan;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rohan;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rohan; 