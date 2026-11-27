CREATE TABLE IF NOT EXISTS bronze.fire_ingestion_coverage (

    coverage_date DATE NOT NULL,

    source VARCHAR(30) NOT NULL,

    bounding_box VARCHAR(100) NOT NULL,

    records_extracted INTEGER NOT NULL
        CHECK (records_extracted >= 0),

    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        coverage_date,
        source,
        bounding_box
    )

);