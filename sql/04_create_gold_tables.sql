CREATE TABLE IF NOT EXISTS gold.mart_fire_monitoring (
    province_id INTEGER NOT NULL
        REFERENCES silver.dim_location(province_id),
    observation_date DATE NOT NULL,
    province_name VARCHAR(100) NOT NULL,
    island VARCHAR(50) NOT NULL,

    -- Fire metrics
    hotspot_count INTEGER,
    low_confidence_count INTEGER,
    nominal_confidence_count INTEGER,
    high_confidence_count INTEGER,
    avg_frp DOUBLE PRECISION,

    -- Daily weather
    avg_temperature DOUBLE PRECISION,
    avg_precipitation DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,

    -- Lagged temperature
    avg_temperature_7d DOUBLE PRECISION,
    avg_temperature_30d DOUBLE PRECISION,
    avg_temperature_60d DOUBLE PRECISION,

    -- Lagged cumulative rainfall
    cumulative_rainfall_7d DOUBLE PRECISION,
    cumulative_rainfall_30d DOUBLE PRECISION,
    cumulative_rainfall_60d DOUBLE PRECISION,

    -- Lagged humidity
    avg_humidity_7d DOUBLE PRECISION,
    avg_humidity_30d DOUBLE PRECISION,
    avg_humidity_60d DOUBLE PRECISION,

    -- CPO prices
    cpo_price_myr NUMERIC(12,2),
    cpo_price_date DATE,
    cpo_price_30d NUMERIC(12,2),
    cpo_price_90d NUMERIC(12,2),
    cpo_price_180d NUMERIC(12,2),

    -- CPO price changes
    cpo_change_30d DOUBLE PRECISION,
    cpo_change_90d DOUBLE PRECISION,
    cpo_change_180d DOUBLE PRECISION,

    -- Data quality
    weather_sampling_points INTEGER,
    weather_complete BOOLEAN,
    fire_coverage_complete BOOLEAN,

    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (province_id, observation_date)
);
-- Migrate existing Gold table to the updated schema
ALTER TABLE gold.mart_fire_monitoring
    ADD COLUMN IF NOT EXISTS avg_temperature_60d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cumulative_rainfall_60d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS avg_humidity_60d DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cpo_price_30d NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS cpo_price_90d NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS cpo_price_180d NUMERIC(12,2);

-- Remove obsolete 90-day weather features
ALTER TABLE gold.mart_fire_monitoring
    DROP COLUMN IF EXISTS avg_temperature_90d,
    DROP COLUMN IF EXISTS cumulative_rainfall_90d,
    DROP COLUMN IF EXISTS avg_humidity_90d;