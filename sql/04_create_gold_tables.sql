CREATE TABLE IF NOT EXISTS gold.mart_fire_monitoring (

    province_id INTEGER NOT NULL
        REFERENCES silver.dim_location(province_id),

    observation_date DATE NOT NULL,

    -- Geographic information
    province_name VARCHAR(100) NOT NULL,
    island VARCHAR(50) NOT NULL,

    -- Hotspot observations
    hotspot_count INTEGER,
    low_confidence_count INTEGER,
    nominal_confidence_count INTEGER,
    high_confidence_count INTEGER,
    avg_frp DOUBLE PRECISION,

    -- Current weather
    avg_temperature DOUBLE PRECISION,
    avg_precipitation DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,

    -- Historical temperature
    avg_temperature_7d DOUBLE PRECISION,
    avg_temperature_30d DOUBLE PRECISION,
    avg_temperature_90d DOUBLE PRECISION,

    -- Historical rainfall
    cumulative_rainfall_7d DOUBLE PRECISION,
    cumulative_rainfall_30d DOUBLE PRECISION,
    cumulative_rainfall_90d DOUBLE PRECISION,

    -- Historical humidity
    avg_humidity_7d DOUBLE PRECISION,
    avg_humidity_30d DOUBLE PRECISION,
    avg_humidity_90d DOUBLE PRECISION,

    -- CPO prices
    cpo_price_myr NUMERIC(12, 2),
    cpo_price_date DATE,
    cpo_change_30d DOUBLE PRECISION,
    cpo_change_90d DOUBLE PRECISION,
    cpo_change_180d DOUBLE PRECISION,

    -- Data quality indicators
    weather_sampling_points INTEGER,
    weather_complete BOOLEAN,
    fire_coverage_complete BOOLEAN,

    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (province_id, observation_date)
);