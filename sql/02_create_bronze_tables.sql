CREATE TABLE IF NOT EXISTS bronze.raw_fire (
    id BIGSERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bright_ti4 DOUBLE PRECISION,
    scan DOUBLE PRECISION,
    track DOUBLE PRECISION,
    acq_date DATE,
    acq_time VARCHAR(4),
    satellite VARCHAR(10),
    instrument VARCHAR(20),
    confidence VARCHAR(10),
    version VARCHAR(20),
    bright_ti5 DOUBLE PRECISION,
    frp DOUBLE PRECISION,
    daynight VARCHAR(1),
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (latitude, longitude, acq_date, acq_time, satellite, instrument, version)
);
CREATE TABLE IF NOT EXISTS bronze.raw_weather (
    id BIGSERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    weather_date DATE NOT NULL,
    temperature_2m_mean DOUBLE PRECISION,
    precipitation_sum DOUBLE PRECISION,
    relative_humidity_2m_mean DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (latitude, longitude, weather_date)
);
CREATE TABLE IF NOT EXISTS bronze.raw_cpo (
    id BIGSERIAL PRIMARY KEY,
    price_date DATE NOT NULL UNIQUE,
    price_raw TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);