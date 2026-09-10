CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    energy_kw DOUBLE PRECISION NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sensor_timestamp
ON sensor_readings(timestamp);

CREATE INDEX IF NOT EXISTS idx_sensor_device_timestamp
ON sensor_readings(device_id,timestamp DESC);

CREATE TABLE IF NOT EXISTS hourly_sensor_summary (
    hour TIMESTAMPTZ,
    device_id TEXT,
    avg_energy_kw DOUBLE PRECISION,
    max_energy_kw DOUBLE PRECISION,
    samples BIGINT,
    PRIMARY KEY(hour,device_id)
);

CREATE TABLE IF NOT EXISTS anomaly_flags (
    timestamp TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    energy_kw DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY(timestamp,device_id,reason)
);
