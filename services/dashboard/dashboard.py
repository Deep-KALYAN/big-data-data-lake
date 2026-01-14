import os
import json
import streamlit as st
from hdfs import InsecureClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/opensky/processed")

# ---------------------------------------------------------------------
# STREAMLIT SETUP
# ---------------------------------------------------------------------
st.set_page_config(page_title="OpenSky Offline Dashboard", layout="wide")
st.title("🌍 ✈ OpenSky Global Intelligence Dashboard")
st.caption("All analytics and maps rendered using Plotly")

# ---------------------------------------------------------------------
# HDFS CONNECTION
# ---------------------------------------------------------------------
client = InsecureClient(HDFS_URL, user=HDFS_USER)

try:
    files = sorted([f for f in client.list(PROCESSED_DIR) if f.endswith(".csv")])
except Exception as e:
    st.error(f"❌ Cannot read HDFS folder: {e}")
    st.stop()

if not files:
    st.warning("No processed CSV files found.")
    st.stop()

# ---------------------------------------------------------------------
# LOAD RECENT FILES
# ---------------------------------------------------------------------
MAX_FILES = 30
recent_files = files[-MAX_FILES:]

dfs = []
timestamps = []

for fname in recent_files:
    try:
        with client.read(f"{PROCESSED_DIR}/{fname}", encoding="utf-8") as reader:
            df = pd.read_csv(reader)
        dfs.append(df)

        # extract 20251123_193501 from opensky_processed_...
        ts = fname.split("processed_")[-1].replace(".csv", "")
        timestamps.append(ts)

    except Exception as e:
        print("Error loading:", fname, e)

if not dfs:
    st.error("❌ No CSVs could be loaded.")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# Keep only rows with valid coords
df = df.dropna(subset=["longitude", "latitude"])

# ---------------------------------------------------------------------
# PARSE TIMESTAMP RANGE
# ---------------------------------------------------------------------
def parse_ts(ts):
    try:
        return datetime.strptime(ts, "%Y%m%d_%H%M%S")
    except:
        return None

min_dt = parse_ts(min(timestamps))
max_dt = parse_ts(max(timestamps))

if min_dt and max_dt:
    st.info(f"📅 **Data range:** {min_dt.strftime('%Y-%m-%d %H:%M:%S')} → {max_dt.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.info(f"📅 Data range: {min(timestamps)} → {max(timestamps)}")

# ---------------------------------------------------------------------
# EXTRACT UTC HOUR
# ---------------------------------------------------------------------
df["hour"] = pd.to_datetime(df["last_contact"], unit="s").dt.hour

# ======================================================

def infer_continent(lat, lon):
    """
    Infer continent/region based on latitude and longitude.
    Returns one of: 'North America', 'South America', 'Europe', 'Africa', 'Asia', 'Oceania'
    """

    if -170 <= lon <= -30 and 5 <= lat <= 85:
        return "North America"
    elif -85 <= lon <= -30 and -60 <= lat <= 15:
        return "South America"
    elif -10 <= lon <= 60 and 35 <= lat <= 72:
        return "Europe"
    elif -20 <= lon <= 55 and -35 <= lat <= 35:
        return "Africa"
    elif 60 <= lon <= 180 and -10 <= lat <= 55:
        return "Asia"
    elif 110 <= lon <= 180 and -50 <= lat <= 0:
        return "Oceania"
    else:
        return "Other"
df["continent"] = df.apply(lambda row: infer_continent(row["latitude"], row["longitude"]), axis=1)


# ---------------------------------------------------------------------
# 🌍 OFFLINE GLOBAL AIR TRAFFIC HEATMAP (Plotly Density Map)
# ---------------------------------------------------------------------
st.subheader("🌐 Global Air Traffic Heatmap")

heat_df = df.sample(min(len(df), 10000))  # limit for speed

fig_heat = px.density_mapbox(
    heat_df,
    lat="latitude",
    lon="longitude",
    radius=7,
    zoom=1,
    center=dict(lat=20, lon=0),
    mapbox_style="open-street-map",  # rendered offline by Plotly
    hover_name="callsign",
    hover_data={
        "origin_country": True,       # show country
        "continent": True,            # show region
        "latitude": False,            # hide lat/lon if desired
        "longitude": False
    }
)

# Forces offline rendering (no internet call)
fig_heat.update_layout(mapbox_accesstoken=None)

st.plotly_chart(fig_heat, use_container_width=True)


# ---------------------------------------------------------------------
# 🛰 LIVE AIRCRAFT POSITIONS (Offline World Map)
# ---------------------------------------------------------------------
st.subheader("🛰 Live Aircraft Positions (Offline)")

scatter_df = df.sample(min(len(df), 7000))

fig_scatter = px.scatter_geo(
    scatter_df,
    lat="latitude",
    lon="longitude",
    hover_name="callsign",
    hover_data=["origin_country"],
    projection="natural earth",
)

fig_scatter.update_traces(marker=dict(size=3, color="orange"))

st.plotly_chart(fig_scatter, use_container_width=True)


# ---------------------------------------------------------------------
# 📈 BUSIEST AIR ROUTES (TOP CALLSIGNS)
# ---------------------------------------------------------------------
st.subheader("📈 Busiest Air Routes (Top Callsigns)")

route_df = (
    df["callsign"]
    .fillna("UNKNOWN")
    .value_counts()
    .head(15)
    .reset_index()
)
route_df.columns = ["callsign", "count"]

fig_routes = px.bar(route_df, x="callsign", y="count", title="Top Callsigns")
st.plotly_chart(fig_routes, use_container_width=True)


# ---------------------------------------------------------------------
# 🌍 COUNTRIES WITH MOST FLIGHTS
# ---------------------------------------------------------------------
st.subheader("🌍 Countries With Highest Flight Volume")

country_df = (
    df["origin_country"]
    .value_counts()
    .head(15)
    .reset_index()
)
country_df.columns = ["country", "count"]

fig_countries = px.bar(country_df, x="country", y="count", title="Top Origin Countries")
st.plotly_chart(fig_countries, use_container_width=True)


# ---------------------------------------------------------------------
# 🌎 INBOUND TRAFFIC BY REGION (Offline)
# ---------------------------------------------------------------------
st.subheader("🌎 Inbound Flights by Region")

# def infer_continent(lon):
#     if lon < -30:
#         return "Americas"
#     if -30 <= lon < 60:
#         return "Europe/Africa"
#     return "Asia/Oceania"

# df["continent"] = df["longitude"].apply(infer_continent)

continent_df = df["continent"].value_counts().reset_index()
continent_df.columns = ["continent", "count"]

fig_continent = px.bar(continent_df, x="continent", y="count")
st.plotly_chart(fig_continent, use_container_width=True)


# ---------------------------------------------------------------------
# ⏱️ TIME-OF-DAY TRAFFIC LOAD PER CONTINENT
# ---------------------------------------------------------------------
st.subheader("⏱️ Time-of-Day Traffic Load by Continent (Offline)")

traffic_by_hour = (
    df.groupby(["continent", "hour"])["icao24"]
    .count()
    .reset_index(name="flight_count")
)

for continent in sorted(df["continent"].unique()):
    st.markdown(f"### 🌎 {continent}")
    sub = traffic_by_hour[traffic_by_hour["continent"] == continent]

    fig_hour = px.line(sub, x="hour", y="flight_count")
    st.plotly_chart(fig_hour, use_container_width=True)


# ---------------------------------------------------------------------
# ⏰ GLOBAL TRAFFIC BY HOUR
# ---------------------------------------------------------------------
st.subheader("⏰ Global Traffic By Hour (UTC)")

hour_df = df["hour"].value_counts().sort_index().reset_index()
hour_df.columns = ["hour", "count"]

fig_hour_global = px.line(hour_df, x="hour", y="count")
st.plotly_chart(fig_hour_global, use_container_width=True)


# ---------------------------------------------------------------------
# RAW DATA SAMPLE
# ---------------------------------------------------------------------
with st.expander("📄 View Sample Data (First 200 Rows)"):
    st.dataframe(df.head(200))

st.success("Dashboard loaded successfully (offline mode).")

# ---------------------------------------------------------------------
# END OF FILE