-- Create users (if they don't exist)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'poorna') THEN
      CREATE USER poorna WITH PASSWORD 'poorna';
   END IF;
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'sanya') THEN
      CREATE USER sanya WITH PASSWORD 'sanya';
   END IF;
END
$do$;

-- Create database (if it doesn't exist)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database
      WHERE  datname = 'bitsight') THEN
      CREATE DATABASE bitsight;
   END IF;
END
$do$;

-- Grant privileges to users
GRANT ALL PRIVILEGES ON DATABASE bitsight TO poorna;
GRANT ALL PRIVILEGES ON DATABASE bitsight TO sanya;

-- Connect to the database
\c bitsight

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

CREATE TABLE IF NOT EXISTS stock_aggregates (
	id SERIAL PRIMARY KEY,
	start_datetime TIMESTAMP WITHOUT TIME ZONE,
	end_datetime TIMESTAMP WITHOUT TIME ZONE,
	average_price DECIMAL,
	min_price DECIMAL,
	max_price DECIMAL,
	price_stddev DECIMAL,
	total_volume DECIMAL,
	average_volume DECIMAL,
	min_volume DECIMAL,
	max_volume DECIMAL,
	total_records INTEGER
);

-- Grant table privileges to users
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO poorna;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sanya;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO poorna;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sanya;