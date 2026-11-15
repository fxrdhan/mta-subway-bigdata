"""Quality rules and cleaning from notebook section IV, expressed as DuckDB SQL.

Every query joins a `station_mode` relation (station_complex_id, station_mode) that holds
the dominant transit_mode of each station complex over all data.
"""
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from .soda import COLUMNS

NY_TZ = ZoneInfo("America/New_York")
NYC_LAT, NYC_LON = (40.47, 40.92), (-74.26, -73.70)

# tag -> severity; "error" rows must be repaired, "warning" rows are only marked.
SEVERITY = {
    "missing_value": "error",
    "invalid_category": "error",
    "nonpositive_ridership": "error",
    "negative_transfers": "error",
    "transfers_exceed_ridership": "error",
    "non_integer_count": "error",
    "outside_nyc": "error",
    "not_on_hour": "error",
    "fare_class_payment_mismatch": "error",
    "station_mode_mismatch": "error",
    "georeference_mismatch": "error",
    "dst_transition_hour": "warning",
}

# Violations the cleaning step repairs; any other "error" violation stops the run so the
# serving tables keep the last valid data (design report, Tabel 2.4).
REPAIRED = frozenset({"station_mode_mismatch", "negative_transfers"})
BLOCKING = frozenset(tag for tag, severity in SEVERITY.items() if severity == "error") - REPAIRED


def dst_transition_hours(first: dt.datetime, last: dt.datetime) -> list[dt.datetime]:
    """Local New York hours in [first, last] that do not exist or occur twice because of DST."""
    found, hour = [], first.replace(minute=0, second=0, microsecond=0)
    while hour <= last:
        if hour.replace(tzinfo=NY_TZ, fold=0).utcoffset() != hour.replace(tzinfo=NY_TZ, fold=1).utcoffset():
            found.append(hour)
        hour += dt.timedelta(hours=1)
    return found


def _in_hours(hours: list[dt.datetime]) -> str:
    if not hours:
        return "FALSE"
    return "transit_timestamp IN (" + ", ".join(f"TIMESTAMP '{h:%Y-%m-%d %H:%M:%S}'" for h in hours) + ")"


def violation_sql(dst_hours: list[dt.datetime]) -> dict[str, str]:
    """SQL condition per tag that is true when a row violates the rule."""
    point = "string_split(trim(georeference, 'POINT ()'), ' ')"  # [longitude, latitude] as text
    conditions = {
        "missing_value": " OR ".join(f"{c} IS NULL" for c in COLUMNS),
        "invalid_category": "transit_mode NOT IN ('subway', 'staten_island_railway', 'tram') "
                            "OR payment_method NOT IN ('omny', 'metrocard') "
                            "OR borough NOT IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island')",
        "nonpositive_ridership": "ridership <= 0",
        "negative_transfers": "transfers < 0",
        "transfers_exceed_ridership": "transfers > ridership",
        "non_integer_count": "ridership <> floor(ridership) OR transfers <> floor(transfers)",
        "outside_nyc": f"latitude NOT BETWEEN {NYC_LAT[0]} AND {NYC_LAT[1]} "
                       f"OR longitude NOT BETWEEN {NYC_LON[0]} AND {NYC_LON[1]}",
        "not_on_hour": "date_trunc('hour', transit_timestamp) <> transit_timestamp",
        "fare_class_payment_mismatch": "lower(split_part(fare_class_category, ' - ', 1)) <> payment_method",
        "station_mode_mismatch": "transit_mode <> station_mode",
        "georeference_mismatch": f"CAST({point}[1] AS DOUBLE) <> longitude OR CAST({point}[2] AS DOUBLE) <> latitude",
        "dst_transition_hour": _in_hours(dst_hours),
    }
    assert conditions.keys() == SEVERITY.keys(), "every rule needs exactly one condition"
    return conditions


def tag_counts_sql(source: str, dst_hours: list[dt.datetime]) -> str:
    """Rows per violated tag in `source` (one scan for all rules)."""
    tags = ", ".join(f"CASE WHEN {cond} THEN '{tag}' END" for tag, cond in violation_sql(dst_hours).items())
    return (
        f"SELECT tag, count(*) AS rows FROM ("
        f"SELECT unnest(list_distinct([{tags}])) AS tag FROM {source} JOIN station_mode USING (station_complex_id)"
        f") GROUP BY tag"
    )


def clean_sql(source: str, dst_hours: list[dt.datetime]) -> str:
    """Repaired rows of `source`: dominant station mode, whole-number counts, negative transfers set to 0."""
    repairs = {
        "transit_mode": "station_mode",
        "ridership": "CAST(ridership AS INTEGER)",
        "transfers": "CAST(greatest(transfers, 0) AS INTEGER)",
    }
    columns = ", ".join(f"{repairs.get(c, c)} AS {c}" for c in COLUMNS)
    return (
        f"SELECT {columns}, {_in_hours(dst_hours)} AS is_dst_transition "
        f"FROM {source} JOIN station_mode USING (station_complex_id)"
    )
