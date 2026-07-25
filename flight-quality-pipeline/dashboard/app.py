"""Streamlit dashboard reading the dbt marts in BigQuery: historical (OpenAQ) air-quality KPIs,
live (OpenSky) flight-tracking KPIs, and combined flight-activity/air-quality correlation charts.
"""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ["BIGQUERY_DATASET"]

# Reused from the project's validated categorical/sequential palette (see the dataviz skill).
COUNTRY_COLORS = {"US": "#2a78d6", "PK": "#1baf7a"}
SEQUENTIAL_BLUE = "#2a78d6"

st.set_page_config(page_title="Air Quality & Flight Tracking Dashboard", layout="wide")


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str) -> pd.DataFrame:
    return get_bq_client().query(sql).to_dataframe()


@st.cache_data(ttl=60)
def load_daily_summary() -> pd.DataFrame:
    return run_query(f"select * from `{PROJECT_ID}.{DATASET}.mart_openaq_daily_summary`")


@st.cache_data(ttl=60)
def load_most_polluted_days() -> pd.DataFrame:
    return run_query(f"select * from `{PROJECT_ID}.{DATASET}.mart_openaq_most_polluted_days`")


@st.cache_data(ttl=30)
def load_latest_flights() -> pd.DataFrame:
    return run_query(f"select * from `{PROJECT_ID}.{DATASET}.mart_opensky_latest`")


@st.cache_data(ttl=60)
def load_correlation() -> pd.DataFrame:
    return run_query(f"select * from `{PROJECT_ID}.{DATASET}.mart_flight_air_quality_correlation`")


def format_pollutant(value: float) -> str:
    # Gas pollutants (NO2, O3, SO2, CO) are reported in ppm - values like 0.008 round straight to
    # "0.0" at one decimal place, which reads as missing/broken data rather than a real, tiny
    # concentration. PM2.5/PM10 (µg/m³) are large enough that one decimal place stays meaningful.
    if pd.isna(value):
        return "—"
    return f"{value:.4f}" if abs(value) < 1 else f"{value:.1f}"


st.title("Air Quality & Flight Tracking Dashboard")

tab_historical, tab_live, tab_combined = st.tabs(
    ["Historical (OpenAQ)", "Live (OpenSky)", "Combined"]
)

with tab_historical:
    daily = load_daily_summary()
    if daily.empty:
        st.info("No OpenAQ data yet - run the batch pipeline (dlt -> Spark -> BigQuery) first.")
    else:
        pollutants = ["pm25", "pm10", "no2", "o3"]
        cols = st.columns(len(pollutants))
        for col, pollutant in zip(cols, pollutants):
            avg_value = daily.loc[daily["parameter"] == pollutant, "avg_value"].mean()
            col.metric(pollutant.upper(), format_pollutant(avg_value))

        st.subheader("Pollution trend")
        parameter = st.selectbox("Parameter", pollutants, key="trend_parameter")
        trend = (
            daily[daily["parameter"] == parameter]
            .groupby("reading_date", as_index=False)["avg_value"]
            .mean()
            .sort_values("reading_date")
        )
        if trend.empty:
            st.info(f"No {parameter.upper()} readings yet.")
        else:
            fig = px.line(trend, x="reading_date", y="avg_value", markers=True)
            fig.update_traces(line_color=SEQUENTIAL_BLUE)
            fig.update_layout(yaxis_title=parameter.upper(), xaxis_title="Date")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Most polluted days (by PM2.5)")
        polluted = load_most_polluted_days()
        if polluted.empty:
            st.info("No PM2.5 readings yet.")
        else:
            polluted = polluted.copy()
            polluted["label"] = polluted["location_name"] + " (" + polluted["reading_date"].astype(str) + ")"
            fig2 = px.bar(polluted, x="label", y="avg_pm25")
            fig2.update_traces(marker_color=SEQUENTIAL_BLUE)
            fig2.update_layout(xaxis_title="Location / Date", yaxis_title="Avg PM2.5")
            st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(polluted.drop(columns="label"), use_container_width=True)

with tab_live:
    if st.button("Refresh now"):
        load_latest_flights.clear()
    latest = load_latest_flights()
    if latest.empty:
        st.info("No OpenSky data yet - start the producer + consumer + Spark batch job first.")
    else:
        st.metric("Aircraft currently tracked", len(latest))
        for _, row in latest.sort_values("origin_country").iterrows():
            st.subheader(f"{row['callsign'] or row['icao24']} ({row['origin_country']})")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Altitude (m)", f"{row['baro_altitude']:.0f}" if pd.notna(row["baro_altitude"]) else "—")
            c2.metric("Ground speed (m/s)", f"{row['velocity']:.1f}" if pd.notna(row["velocity"]) else "—")
            c3.metric("Heading (°)", f"{row['true_track']:.0f}" if pd.notna(row["true_track"]) else "—")
            c4.metric("Vertical rate (m/s)", f"{row['vertical_rate']:.1f}" if pd.notna(row["vertical_rate"]) else "—")
            c5.metric("On ground", "Yes" if row["on_ground"] else "No")

with tab_combined:
    corr = load_correlation()
    if corr.empty:
        st.info("Need both OpenAQ and OpenSky data loaded before combined KPIs can be shown.")
    else:
        pairs = [
            ("flight_count", "avg_pm25", "Flight count vs PM2.5", "Flights tracked", "Avg PM2.5"),
            ("avg_altitude", "avg_pm25", "Avg altitude vs PM2.5", "Avg altitude (m)", "Avg PM2.5"),
            ("avg_velocity", "avg_pm10", "Avg ground speed vs PM10", "Avg ground speed (m/s)", "Avg PM10"),
            ("flight_count", "avg_no2", "Flight count vs NO2", "Flights tracked", "Avg NO2"),
        ]
        cols = st.columns(2)
        for i, (x, y, title, x_label, y_label) in enumerate(pairs):
            data = corr.dropna(subset=[x, y])
            fig = px.scatter(
                data, x=x, y=y, color="country", color_discrete_map=COUNTRY_COLORS, title=title
            )
            fig.update_layout(xaxis_title=x_label, yaxis_title=y_label, legend_title="Country")
            cols[i % 2].plotly_chart(fig, use_container_width=True)
