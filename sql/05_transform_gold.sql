BEGIN;

DELETE FROM gold.mart_fire_monitoring;

WITH date_spine AS (

    SELECT generate_series(
        '2025-01-01'::DATE,
        (
        SELECT MAX(coverage_date)
        FROM bronze.fire_ingestion_coverage
        WHERE bounding_box = '94,-7,120,8'
        ),
        INTERVAL '1 day'
    )::DATE AS observation_date

),

province_dates AS (

    SELECT
        l.province_id,
        l.province_name,
        l.island,
        d.observation_date,

        (
            EXISTS (
                SELECT 1
                FROM bronze.fire_ingestion_coverage c
                WHERE c.coverage_date = d.observation_date - 1
                  AND c.bounding_box = '94,-7,120,8'
            )

            AND

            EXISTS (
                SELECT 1
                FROM bronze.fire_ingestion_coverage c
                WHERE c.coverage_date = d.observation_date
                  AND c.bounding_box = '94,-7,120,8'
            )

            AND d.observation_date <=
                (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::DATE - 2

        ) AS fire_coverage_complete

    FROM silver.dim_location l
    CROSS JOIN date_spine d

),

daily_data AS (

    SELECT
        p.*,

        CASE WHEN p.fire_coverage_complete
            THEN COALESCE(f.hotspot_count, 0)
        END AS hotspot_count,

        CASE WHEN p.fire_coverage_complete
            THEN COALESCE(f.low_confidence_count, 0)
        END AS low_confidence_count,

        CASE WHEN p.fire_coverage_complete
            THEN COALESCE(f.nominal_confidence_count, 0)
        END AS nominal_confidence_count,

        CASE WHEN p.fire_coverage_complete
            THEN COALESCE(f.high_confidence_count, 0)
        END AS high_confidence_count,

        CASE WHEN p.fire_coverage_complete
            THEN f.avg_frp
        END AS avg_frp,

        w.avg_temperature,
        w.avg_precipitation,
        w.avg_humidity,

        w.sampling_points_count,

        COALESCE(
            w.valid_temperature_points = 10
            AND w.valid_precipitation_points = 10
            AND w.valid_humidity_points = 10,
            FALSE
        ) AS weather_complete

    FROM province_dates p

    LEFT JOIN silver.fact_daily_fire f
        ON p.province_id = f.province_id
        AND p.observation_date = f.fire_date

    LEFT JOIN silver.fact_daily_weather w
        ON p.province_id = w.province_id
        AND p.observation_date = w.weather_date

),

weather_features AS (

    SELECT
        *,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w7 = 7
        THEN AVG(avg_temperature) OVER w7
        END AS avg_temperature_7d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w30 = 30
        THEN AVG(avg_temperature) OVER w30
        END AS avg_temperature_30d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w90 = 90
        THEN AVG(avg_temperature) OVER w90
        END AS avg_temperature_90d,


        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w7 = 7
        THEN SUM(avg_precipitation) OVER w7
        END AS cumulative_rainfall_7d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w30 = 30
        THEN SUM(avg_precipitation) OVER w30
        END AS cumulative_rainfall_30d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w90 = 90
        THEN SUM(avg_precipitation) OVER w90
        END AS cumulative_rainfall_90d,


        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w7 = 7
        THEN AVG(avg_humidity) OVER w7
        END AS avg_humidity_7d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w30 = 30
        THEN AVG(avg_humidity) OVER w30
        END AS avg_humidity_30d,

        CASE WHEN COUNT(*) FILTER (
            WHERE weather_complete
        ) OVER w90 = 90
        THEN AVG(avg_humidity) OVER w90
        END AS avg_humidity_90d

    FROM daily_data

    WINDOW

        w7 AS (
            PARTITION BY province_id
            ORDER BY observation_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ),

        w30 AS (
            PARTITION BY province_id
            ORDER BY observation_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ),

        w90 AS (
            PARTITION BY province_id
            ORDER BY observation_date
            ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
        )

)

INSERT INTO gold.mart_fire_monitoring (

    province_id,
    observation_date,
    province_name,
    island,

    hotspot_count,
    low_confidence_count,
    nominal_confidence_count,
    high_confidence_count,
    avg_frp,

    avg_temperature,
    avg_precipitation,
    avg_humidity,

    avg_temperature_7d,
    avg_temperature_30d,
    avg_temperature_90d,

    cumulative_rainfall_7d,
    cumulative_rainfall_30d,
    cumulative_rainfall_90d,

    avg_humidity_7d,
    avg_humidity_30d,
    avg_humidity_90d,

    cpo_price_myr,
    cpo_price_date,
    cpo_change_30d,
    cpo_change_90d,
    cpo_change_180d,

    weather_sampling_points,
    weather_complete,
    fire_coverage_complete

)

SELECT

    w.province_id,
    w.observation_date,
    w.province_name,
    w.island,

    w.hotspot_count,
    w.low_confidence_count,
    w.nominal_confidence_count,
    w.high_confidence_count,
    w.avg_frp,

    w.avg_temperature,
    w.avg_precipitation,
    w.avg_humidity,

    w.avg_temperature_7d,
    w.avg_temperature_30d,
    w.avg_temperature_90d,

    w.cumulative_rainfall_7d,
    w.cumulative_rainfall_30d,
    w.cumulative_rainfall_90d,

    w.avg_humidity_7d,
    w.avg_humidity_30d,
    w.avg_humidity_90d,

    cpo_current.cpo_price_myr,
    cpo_current.price_date,

    100.0 * (
        cpo_current.cpo_price_myr
        / NULLIF(cpo_30.cpo_price_myr, 0) - 1
    ),

    100.0 * (
        cpo_current.cpo_price_myr
        / NULLIF(cpo_90.cpo_price_myr, 0) - 1
    ),

    100.0 * (
        cpo_current.cpo_price_myr
        / NULLIF(cpo_180.cpo_price_myr, 0) - 1
    ),

    w.sampling_points_count,
    w.weather_complete,

    w.fire_coverage_complete

FROM weather_features w

LEFT JOIN LATERAL (

    SELECT price_date, cpo_price_myr
    FROM silver.fact_daily_cpo
    WHERE price_date < w.observation_date
    ORDER BY price_date DESC
    LIMIT 1

) cpo_current ON TRUE

LEFT JOIN LATERAL (

    SELECT cpo_price_myr
    FROM silver.fact_daily_cpo
    WHERE price_date <= w.observation_date - 31
    ORDER BY price_date DESC
    LIMIT 1

) cpo_30 ON TRUE

LEFT JOIN LATERAL (

    SELECT cpo_price_myr
    FROM silver.fact_daily_cpo
    WHERE price_date <= w.observation_date - 91
    ORDER BY price_date DESC
    LIMIT 1

) cpo_90 ON TRUE

LEFT JOIN LATERAL (

    SELECT cpo_price_myr
    FROM silver.fact_daily_cpo
    WHERE price_date <= w.observation_date - 181
    ORDER BY price_date DESC
    LIMIT 1

) cpo_180 ON TRUE;

COMMIT;