"""Dashboard Subway NYC: ridership patterns, stations, hourly patterns, a ridership prediction, and pipeline status.

Reads only from PostgreSQL with a read-only account (libpq environment variables PGHOST, PGUSER, ...).
Run: streamlit run streamlit_app.py
"""
from __future__ import annotations

import datetime as dt
import io

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st
from pandas.tseries.holiday import USFederalHolidayCalendar

st.set_page_config(page_title="Dashboard Subway NYC", page_icon="🚇", layout="wide")
DAY_NAMES = {1: "Senin", 2: "Selasa", 3: "Rabu", 4: "Kamis", 5: "Jumat", 6: "Sabtu", 7: "Minggu"}
COLORS = {"OMNY": "#1EA185", "MetroCard": "#F29B26", "weekday": "#445469", "weekend": "#BD392F"}


@st.cache_resource
def connection() -> psycopg.Connection:
    return psycopg.connect(application_name="mta-app", autocommit=True)


@st.cache_data(ttl=600, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with connection().cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


@st.cache_resource(ttl=3600, show_spinner=False)
def current_model() -> dict | None:
    rows = query("SELECT artifact FROM ml_model WHERE is_current ORDER BY trained_at DESC LIMIT 1")
    return joblib.load(io.BytesIO(bytes(rows.iloc[0, 0]))) if len(rows) else None


def fmt(n: float) -> str:
    return f"{n:,.0f}".replace(",", ".")


def overview() -> None:
    daily = query("SELECT day, sum(ridership)::bigint AS total, sum(omny_ridership)::bigint AS omny "
                  "FROM fact_ridership_daily_station GROUP BY day ORDER BY day")
    daily["day"] = pd.to_datetime(daily["day"])
    last = daily["day"].max()
    week = daily[daily["day"] > last - pd.Timedelta(days=7)]["total"].sum()
    prev = daily[(daily["day"] <= last - pd.Timedelta(days=7)) & (daily["day"] > last - pd.Timedelta(days=14))]["total"].sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Data terakhir", f"{last:%d %b %Y}")
    c2.metric("Penumpang 7 hari terakhir", fmt(week), f"{(week / prev - 1) * 100:+.1f}% dari minggu sebelumnya" if prev else None)
    c3.metric("Rata-rata per hari (30 hari)", fmt(daily.tail(30)["total"].mean()))
    c4.metric("Pangsa OMNY (30 hari)", f"{daily.tail(30)['omny'].sum() / daily.tail(30)['total'].sum() * 100:.1f}%")

    start, end = st.slider("Rentang tanggal", min_value=daily["day"].min().date(), max_value=last.date(),
                           value=(max(daily["day"].min().date(), last.date() - dt.timedelta(days=180)), last.date()),
                           format="DD MMM YYYY")
    view = daily[(daily["day"].dt.date >= start) & (daily["day"].dt.date <= end)].copy()
    view["MetroCard"] = view["total"] - view["omny"]
    long = view.rename(columns={"omny": "OMNY"}).melt("day", ["OMNY", "MetroCard"], var_name="Metode", value_name="Penumpang")
    fig = px.area(long, x="day", y="Penumpang", color="Metode", color_discrete_map=COLORS,
                  labels={"day": "Tanggal"}, title="Penumpang harian menurut metode pembayaran")
    fig.update_layout(hovermode="x unified", legend_title_text="")
    st.plotly_chart(fig, use_container_width=True)


def stations() -> None:
    data = query(
        "SELECT s.station_complex_id, s.station_complex, s.borough, s.latitude, s.longitude, "
        "avg(d.ridership)::double precision AS rata_harian FROM dim_station s JOIN fact_ridership_daily_station d "
        "USING (station_complex_id) WHERE d.day > (SELECT max(day) - 30 FROM fact_ridership_daily_station) "
        "GROUP BY s.station_complex_id, s.station_complex, s.borough, s.latitude, s.longitude "
        "ORDER BY rata_harian DESC")
    left, right = st.columns([3, 2])
    with left:
        fig = px.scatter_map(data, lat="latitude", lon="longitude", size="rata_harian", color="rata_harian",
                             hover_name="station_complex", hover_data={"borough": True, "rata_harian": ":,.0f",
                                                                       "latitude": False, "longitude": False},
                             color_continuous_scale="OrRd", size_max=28, zoom=9.6, height=560,
                             center={"lat": float(data["latitude"].mean()), "lon": float(data["longitude"].mean())},
                             map_style="carto-positron", title="Rata-rata penumpang harian per kompleks stasiun (30 hari)")
        fig.update_layout(coloraxis_colorbar_title="Penumpang")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        top = data.head(15).iloc[::-1]
        fig = px.bar(top, x="rata_harian", y="station_complex", orientation="h", title="15 stasiun tersibuk",
                     labels={"rata_harian": "Rata-rata harian", "station_complex": ""}, color_discrete_sequence=["#BD392F"])
        st.plotly_chart(fig, use_container_width=True)

    choice = st.selectbox("Pilih stasiun", data["station_complex"] + " · " + data["borough"], index=0)
    sid = data.loc[(data["station_complex"] + " · " + data["borough"]) == choice, "station_complex_id"].iloc[0]
    a, b = st.columns(2)
    profile = query("SELECT day_type, hour_of_day, mean_ridership FROM mart_station_hour_profile "
                    "WHERE station_complex_id = %s ORDER BY hour_of_day", (sid,))
    fig = px.line(profile, x="hour_of_day", y="mean_ridership", color="day_type", markers=True,
                  color_discrete_map=COLORS, title="Profil per jam (rata-rata seluruh data)",
                  labels={"hour_of_day": "Jam", "mean_ridership": "Rata-rata penumpang", "day_type": ""})
    a.plotly_chart(fig, use_container_width=True)
    series = query("SELECT day, ridership FROM fact_ridership_daily_station WHERE station_complex_id = %s ORDER BY day", (sid,))
    fig = px.line(series, x="day", y="ridership", title="Penumpang harian", labels={"day": "Tanggal", "ridership": "Penumpang"},
                  color_discrete_sequence=["#445469"])
    b.plotly_chart(fig, use_container_width=True)


def hourly_patterns() -> None:
    weeks = st.select_slider("Periode", options=[4, 8, 13, 26, 52], value=8, format_func=lambda w: f"{w} minggu terakhir")
    heat = query(
        "WITH h AS (SELECT hour, sum(ridership) AS r FROM fact_ridership_hourly "
        "WHERE hour > (SELECT max(hour) FROM fact_ridership_hourly) - make_interval(weeks => %s) GROUP BY hour) "
        "SELECT extract(isodow FROM hour)::int AS dow, extract(hour FROM hour)::int AS jam, avg(r)::double precision AS r "
        "FROM h GROUP BY 1, 2 ORDER BY 1, 2", (weeks,))
    grid = heat.pivot(index="dow", columns="jam", values="r").rename(index=DAY_NAMES)
    fig = px.imshow(grid, aspect="auto", color_continuous_scale="OrRd", labels={"x": "Jam", "y": "", "color": "Penumpang"},
                    title="Rata-rata penumpang seluruh jaringan per jam dan hari")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Jam sibuk pagi dan sore di hari kerja terlihat jelas; akhir pekan lebih landai dan bergeser ke siang.")


def prediction() -> None:
    art = current_model()
    if art is None:
        st.info("Model belum tersedia. Model dilatih mingguan dari data silver oleh job pelatihan.")
        return
    names = art["station_names"]
    options = sorted(art["station_mean"].index, key=lambda s: names.get(s, s))
    c1, c2, c3 = st.columns([3, 2, 2])
    sid = c1.selectbox("Stasiun", options, format_func=lambda s: names.get(s, s))
    day = c2.date_input("Tanggal", value=art["data_end"].date() + dt.timedelta(days=1))
    hour = c3.slider("Jam", 0, 23, 8)

    hours = pd.DataFrame({"hour": range(24)})
    holidays = USFederalHolidayCalendar().holidays(pd.Timestamp(day), pd.Timestamp(day))
    dow = pd.Timestamp(day).isoweekday()
    day_type = "weekend" if dow >= 6 else "weekday"
    x = pd.DataFrame({
        "station_mean": art["station_mean"][sid],
        "profile_mean": [art["profile"].get((sid, day_type, h), np.nan) for h in hours["hour"]],
        "hour": hours["hour"], "dow": dow, "month": day.month, "is_holiday": int(len(holidays) > 0),
    })[art["features"]]
    hours["Prediksi model"] = np.clip(art["model"].predict(x), 0, None)
    hours["Rata-rata profil"] = x["profile_mean"]

    m1, m2, m3 = st.columns(3)
    m1.metric(f"Prediksi pukul {hour:02d}.00", fmt(hours.loc[hour, "Prediksi model"]) + " penumpang")
    m2.metric("Total prediksi sehari", fmt(hours["Prediksi model"].sum()))
    m3.metric("Hari", f"{DAY_NAMES[dow]}{' (libur nasional AS)' if len(holidays) else ''}")
    long = hours.melt("hour", ["Prediksi model", "Rata-rata profil"], var_name="Seri", value_name="Penumpang")
    fig = px.line(long, x="hour", y="Penumpang", color="Seri", markers=True, labels={"hour": "Jam"},
                  color_discrete_sequence=["#BD392F", "#8D98A9"], title=f"Prediksi per jam, {names.get(sid, sid)}, {day:%d %b %Y}")
    fig.add_vline(x=hour, line_dash="dot", line_color="#445469")
    fig.update_layout(legend_title_text="")
    st.plotly_chart(fig, use_container_width=True)
    m = art["metrics"]
    st.caption(f"Model: HistGradientBoosting (loss Poisson) dari data silver {art['data_start']:%d %b %Y} sampai "
               f"{art['data_end']:%d %b %Y}. Uji pada {m['holdout_days']} hari terakhir: MAE {m['mae']:.1f} penumpang per jam "
               f"per stasiun (rata-rata profil saja: {m['baseline_profile_mae']:.1f}), R² {m['r2']:.3f}.")


def pipeline_status() -> None:
    runs = query("SELECT status, started_at, finished_at, days_fetched, rows_fetched, "
                 "coalesce(message, months_built) AS keterangan FROM pipeline_run ORDER BY started_at DESC LIMIT 10")
    st.subheader("Run terakhir")
    st.dataframe(runs, hide_index=True, use_container_width=True)
    quality = query("SELECT to_char(month, 'YYYY-MM') AS bulan, tag, rows FROM quality_month WHERE rows > 0 ORDER BY month, tag")
    st.subheader("Temuan kualitas data per bulan")
    st.dataframe(quality.pivot(index="bulan", columns="tag", values="rows").fillna(0).astype(int), use_container_width=True)
    st.caption("negative_transfers dan station_mode_mismatch diperbaiki otomatis; dst_transition_hour hanya ditandai. "
               "Aturan lain menghentikan run bila dilanggar sehingga dashboard tetap memakai data terakhir yang valid.")


st.title("Dashboard Subway NYC")
st.caption("Data MTA Subway Hourly Ridership (data.ny.gov), diolah pipeline ELT kelompok Smile · Big Data TK47G06")
tabs = st.tabs(["Ringkasan", "Stasiun", "Pola jam", "Prediksi", "Pipeline"])
with tabs[0]:
    overview()
with tabs[1]:
    stations()
with tabs[2]:
    hourly_patterns()
with tabs[3]:
    prediction()
with tabs[4]:
    pipeline_status()
