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
)