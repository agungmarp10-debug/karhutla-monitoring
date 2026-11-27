CREATE TABLE IF NOT EXISTS silver.dim_location (
    province_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    province_name VARCHAR(100) NOT NULL UNIQUE,
    island VARCHAR(50) NOT NULL,
    timezone VARCHAR(50) NOT NULL
);


CREATE TABLE IF NOT EXISTS silver.dim_weather_location (
    weather_location_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    province_id INTEGER NOT NULL
        REFERENCES silver.dim_location(province_id),
    point_number INTEGER NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    UNIQUE (latitude, longitude),
    UNIQUE (province_id, point_number)
);


CREATE TABLE IF NOT EXISTS silver.fact_daily_fire (
    province_id INTEGER NOT NULL
        REFERENCES silver.dim_location(province_id),
    fire_date DATE NOT NULL,
    hotspot_count INTEGER NOT NULL,
    low_confidence_count INTEGER NOT NULL,
    nominal_confidence_count INTEGER NOT NULL,
    high_confidence_count INTEGER NOT NULL,
    avg_frp DOUBLE PRECISION,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (province_id, fire_date)
);


CREATE TABLE IF NOT EXISTS silver.fact_daily_weather (
    province_id INTEGER NOT NULL
        REFERENCES silver.dim_location(province_id),
    weather_date DATE NOT NULL,
    avg_temperature DOUBLE PRECISION,
    avg_precipitation DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    sampling_points_count INTEGER NOT NULL,
    valid_temperature_points INTEGER NOT NULL,
    valid_precipitation_points INTEGER NOT NULL,
    valid_humidity_points INTEGER NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (province_id, weather_date)
);


CREATE TABLE IF NOT EXISTS silver.fact_daily_cpo (
    price_date DATE PRIMARY KEY,
    cpo_price_myr NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);