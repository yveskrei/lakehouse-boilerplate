
-- Drop old schemas
DROP SCHEMA IF EXISTS monitor CASCADE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS monitor;

-- Create tables
CREATE TABLE IF NOT EXISTS monitor.transactions (
    created_ts TIMESTAMP,
    step INTEGER,
    type VARCHAR,
    amount DOUBLE,
    origin_id VARCHAR,
    origin_old_balance DOUBLE,
    origin_new_balance DOUBLE,
    destination_id VARCHAR,
    destination_old_balance DOUBLE,
    destination_new_balance DOUBLE,
    fraud BOOLEAN
);