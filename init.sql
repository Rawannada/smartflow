CREATE TABLE IF NOT EXISTS high_risk_alerts (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR(50),
    speed DOUBLE PRECISION,
    order_id VARCHAR(50),
    value DOUBLE PRECISION,
    event_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);