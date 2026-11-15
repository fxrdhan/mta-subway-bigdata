-- Serving tables in the `mta` database (names follow the week 9 design report).
-- Timestamps without a zone are New York local time, as in the source;
-- timestamptz columns are pipeline bookkeeping in UTC.

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id        uuid PRIMARY KEY,
    job           text NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz,
    status        text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    days_fetched  integer,
    rows_fetched  bigint,
    months_built  text,
    message       text
);

-- Server fingerprint of every day stored in the bronze layer.
CREATE TABLE IF NOT EXISTS ingest_day (
    day                date PRIMARY KEY,
    rows               bigint NOT NULL,
    ridership          bigint NOT NULL,
    transfers          bigint NOT NULL,
    source_updated_at  timestamptz NOT NULL,
    fetched_at         timestamptz NOT NULL
);

-- Bronze version each silver month was built from.
CREATE TABLE IF NOT EXISTS silver_month (
    month              date PRIMARY KEY,
    rows               bigint NOT NULL,
    source_updated_at  timestamptz NOT NULL,
    built_at           timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_station (
    station_complex_id  text PRIMARY KEY,
    station_complex     text NOT NULL,
    borough             text NOT NULL,
    transit_mode        text NOT NULL,
    latitude            double precision,
    longitude           double precision
);

CREATE TABLE IF NOT EXISTS dim_date (
    date         date PRIMARY KEY,
    year         smallint NOT NULL,
    month        smallint NOT NULL,
    day          smallint NOT NULL,
    day_of_week  smallint NOT NULL,  -- ISO: 1 = Monday ... 7 = Sunday
    is_weekend   boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_ridership_hourly (
    hour            timestamp NOT NULL,
    payment_method  text NOT NULL,
    ridership       bigint NOT NULL,
    transfers       bigint NOT NULL,
    PRIMARY KEY (hour, payment_method)
);

CREATE TABLE IF NOT EXISTS fact_ridership_daily_station (
    day                 date NOT NULL,
    station_complex_id  text NOT NULL,
    ridership           bigint NOT NULL,
    transfers           bigint NOT NULL,
    omny_ridership      bigint NOT NULL,
    PRIMARY KEY (day, station_complex_id)
);

CREATE TABLE IF NOT EXISTS fact_ridership_daily_fare_class (
    day                  date NOT NULL,
    fare_class_category  text NOT NULL,
    ridership            bigint NOT NULL,
    PRIMARY KEY (day, fare_class_category)
);

-- Mean hourly ridership per station, weekday vs weekend, over all clean data
-- (hours without any record count as zero).
CREATE TABLE IF NOT EXISTS mart_station_hour_profile (
    station_complex_id  text NOT NULL,
    day_type            text NOT NULL CHECK (day_type IN ('weekday', 'weekend')),
    hour_of_day         smallint NOT NULL,
    mean_ridership      double precision NOT NULL,
    PRIMARY KEY (station_complex_id, day_type, hour_of_day)
);

-- Rule violations found in the bronze data of each month, before cleaning.
CREATE TABLE IF NOT EXISTS quality_month (
    month     date NOT NULL,
    tag       text NOT NULL,
    severity  text NOT NULL,
    rows      bigint NOT NULL,
    PRIMARY KEY (month, tag)
);

-- Ridership models trained from silver; the Streamlit app loads the current one.
-- The same artifact is also stored in MinIO under models/ridership/.
CREATE TABLE IF NOT EXISTS ml_model (
    model_id     uuid PRIMARY KEY,
    name         text NOT NULL,
    trained_at   timestamptz NOT NULL,
    data_start   timestamp NOT NULL,
    data_end     timestamp NOT NULL,
    metrics      jsonb NOT NULL,
    artifact     bytea NOT NULL,
    is_current   boolean NOT NULL DEFAULT false
);
