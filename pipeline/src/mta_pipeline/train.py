"""Trains the hourly ridership model from silver and stores it in MinIO and PostgreSQL.

Features per station and hour: the station's mean hourly ridership, its mean for the same day type and hour
(weekday or weekend profile), hour, ISO day of week, month, and a US federal holiday flag. The target is
total ridership of the station in that hour; hours without any record count as zero.
Usage: python -m mta_pipeline.train [--days 365]
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import sys
import uuid

import joblib
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score

from . import db, lake
from .config import Settings

log = logging.getLogger("mta_pipeline.train")
NAME = "ridership_hourly_hgb"
FEATURES = ["station_mean", "profile_mean", "hour", "dow", "month", "is_holiday"]
HOLDOUT_DAYS = 28


def station_hours(con, settings: Settings, days: int) -> pd.DataFrame:
    """Zero-filled station x hour totals over the last `days` days of silver."""
    sql = f"""
        WITH h AS (SELECT station_complex_id, transit_timestamp AS ts, sum(ridership) AS r
                   FROM read_parquet('{lake.silver_glob(settings)}') GROUP BY ALL),
             span AS (SELECT max(ts) AS b FROM h),
             hours AS (SELECT unnest(generate_series((SELECT b FROM span) - INTERVAL {days} DAY + INTERVAL 1 HOUR,
                                                     (SELECT b FROM span), INTERVAL 1 HOUR)) AS ts),
             st AS (SELECT DISTINCT station_complex_id FROM h)
        SELECT st.station_complex_id AS station, hours.ts, coalesce(h.r, 0)::DOUBLE AS r
        FROM st CROSS JOIN hours LEFT JOIN h ON h.station_complex_id = st.station_complex_id AND h.ts = hours.ts
    """
    return con.execute(sql).df()


def calendar_features(ts: pd.Series) -> pd.DataFrame:
    holidays = USFederalHolidayCalendar().holidays(ts.min().normalize(), ts.max().normalize())
    return pd.DataFrame({
        "hour": ts.dt.hour, "dow": ts.dt.isocalendar().day.astype(int), "month": ts.dt.month,
        "is_holiday": ts.dt.normalize().isin(holidays).astype(int),
    }, index=ts.index)


def day_type(dow: pd.Series) -> pd.Series:
    return np.where(dow >= 6, "weekend", "weekday")


def lookups(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Station mean and station x day type x hour mean, from the training rows only."""
    station_mean = frame.groupby("station")["r"].mean()
    profile = frame.groupby(["station", "day_type", "hour"])["r"].mean()
    return station_mean, profile


def features(frame: pd.DataFrame, station_mean: pd.Series, profile: pd.Series) -> pd.DataFrame:
    keys = pd.MultiIndex.from_arrays([frame["station"], frame["day_type"], frame["hour"]])
    out = frame[["hour", "dow", "month", "is_holiday"]].copy()
    out["station_mean"] = frame["station"].map(station_mean).to_numpy()
    out["profile_mean"] = profile.reindex(keys).to_numpy()
    return out[FEATURES]


def fit(x: pd.DataFrame, y: pd.Series) -> HistGradientBoostingRegressor:
    model = HistGradientBoostingRegressor(loss="poisson", max_iter=300, learning_rate=0.1,
                                          max_leaf_nodes=63, l2_regularization=1.0, random_state=0)
    return model.fit(x, y)


def save(settings: Settings, pg, artifact: dict, blob: bytes) -> None:
    stamp = artifact["trained_at"].strftime("%Y%m%dT%H%M%SZ")
    if settings.on_s3:
        import boto3  # only needed in the cluster
        s3 = boto3.client("s3", endpoint_url=("https://" if settings.s3_use_ssl else "http://") + settings.s3_endpoint,
                          aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key,
                          region_name="us-east-1")
        bucket = settings.lake.removeprefix("s3://")
        for key in (f"models/ridership/{stamp}.joblib", "models/ridership/latest.joblib"):
            s3.put_object(Bucket=bucket, Key=key, Body=blob)
    else:
        import pathlib
        folder = pathlib.Path(settings.lake, "models", "ridership")
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{stamp}.joblib").write_bytes(blob)
    with pg.transaction():
        pg.execute("UPDATE ml_model SET is_current = false WHERE name = %s", (NAME,))
        pg.execute(
            "INSERT INTO ml_model VALUES (%s, %s, %s, %s, %s, %s, %s, true)",
            (uuid.uuid4(), NAME, artifact["trained_at"], artifact["data_start"], artifact["data_end"],
             json.dumps(artifact["metrics"]), blob),
        )
        # Keep the five newest models of this name.
        pg.execute("DELETE FROM ml_model WHERE name = %s AND model_id NOT IN "
                   "(SELECT model_id FROM ml_model WHERE name = %s ORDER BY trained_at DESC LIMIT 5)", (NAME, NAME))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mta-pipeline-train", description=__doc__)
    parser.add_argument("--days", type=int, default=365, help="days of history to train on")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    con, pg = lake.connect(settings), db.connect()
    db.apply_schema(pg)

    frame = station_hours(con, settings, args.days)
    frame = frame.join(calendar_features(frame["ts"]))
    frame["day_type"] = day_type(frame["dow"])
    log.info("training frame: %s rows, %s stations, %s to %s", f"{len(frame):,}", frame["station"].nunique(),
             frame["ts"].min(), frame["ts"].max())

    # Evaluate on the last four weeks with lookups from the earlier weeks only.
    cutoff = frame["ts"].max() - pd.Timedelta(days=HOLDOUT_DAYS)
    train, test = frame[frame["ts"] <= cutoff], frame[frame["ts"] > cutoff]
    sm, prof = lookups(train)
    model = fit(features(train, sm, prof), train["r"])
    x_test = features(test, sm, prof)
    pred = model.predict(x_test)
    metrics = {
        "holdout_days": HOLDOUT_DAYS,
        "mae": round(float(mean_absolute_error(test["r"], pred)), 2),
        "r2": round(float(r2_score(test["r"], pred)), 4),
        "baseline_profile_mae": round(float(mean_absolute_error(test["r"], x_test["profile_mean"])), 2),
        "train_rows": int(len(train)), "test_rows": int(len(test)),
    }
    log.info("holdout metrics: %s", metrics)

    # Final model on all rows.
    sm, prof = lookups(frame)
    model = fit(features(frame, sm, prof), frame["r"])
    names = con.execute(f"SELECT station_complex_id, arg_max(station_complex, transit_timestamp) "
                        f"FROM read_parquet('{lake.silver_glob(settings)}') GROUP BY ALL").fetchall()
    artifact = {
        "name": NAME, "model": model, "features": FEATURES, "station_mean": sm, "profile": prof,
        "station_names": dict(names), "metrics": metrics,
        "trained_at": dt.datetime.now(dt.timezone.utc),
        "data_start": frame["ts"].min().to_pydatetime(), "data_end": frame["ts"].max().to_pydatetime(),
    }
    buffer = io.BytesIO()
    joblib.dump(artifact, buffer, compress=3)
    save(settings, pg, artifact, buffer.getvalue())
    log.info("saved model (%s KB)", f"{len(buffer.getvalue()) // 1024:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
