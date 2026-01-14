import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import plotly.express as px
import numpy as np

# ----------------------------
# Page setup
# ----------------------------
st.set_page_config(layout="wide", page_title="OpenSky Analytics")
st.title("✈️ OpenSky Analytics & Flight Map")
st.caption("Real-time flights from Kafka → HDFS → Streamlit")

status = st.empty()

# ----------------------------
# Load latest processed CSV from HDFS
# ----------------------------
@st.cache_data(ttl=60)
def load_latest_processed_csv():
    try:
        client = InsecureClient("http://namenode:9870", user="root")
        files = client.list("/opensky/processed")
        csv_files = [f for f in files if f.endswith(".csv")]
        if not csv_files:
            return pd.DataFrame()
        latest_file = sorted(csv_files)[-1]
        with client.read(f"/opensky/processed/{latest_file}") as reader:
            df = pd.read_csv(reader)
        return df
    except Exception as e:
        st.error(f"HDFS connection failed: {e}")
        return pd.DataFrame()

# ----------------------------
# Load data
# ----------------------------
with st.spinner("Loading latest flight data..."):
    df = load_latest_processed_csv()

if df.empty:
    status.warning("No processed flight data available yet.")
    st.stop()
else:
    status.success(f"Loaded {len(df)} flights from HDFS")

# ----------------------------
# Sidebar Filters
# ----------------------------
st.sidebar.header("Filters")

# Origin country filter
countries = df["origin_country"].unique()
selected_countries = st.sidebar.multiselect(
    "Aircraft Registration Country", countries, default=list(countries)
)

# Airborne only
airborne_only = st.sidebar.checkbox("Airborne Only", value=True)

# Altitude filter
min_alt, max_alt = st.sidebar.slider(
    "Altitude (meters)", 0, int(df["baro_altitude_m"].max(skipna=True)), 
    (0, int(df["baro_altitude_m"].max(skipna=True)))
)

# Optional: Geographic bounding box filter
lat_range = st.sidebar.slider("Latitude Range", -90.0, 90.0, (-90.0, 90.0))
lon_range = st.sidebar.slider("Longitude Range", -180.0, 180.0, (-180.0, 180.0))

# ----------------------------
# Apply filters
# ----------------------------
filtered_df = df[
    (df["origin_country"].isin(selected_countries)) &
    ((df["on_ground"] == False) if airborne_only else True) &
    (df["baro_altitude_m"].fillna(0).between(min_alt, max_alt)) &
    (df["latitude"].between(lat_range[0], lat_range[1])) &
    (df["longitude"].between(lon_range[0], lon_range[1]))
]

st.sidebar.write(f"Flights displayed: {len(filtered_df)}")

# ----------------------------
# KPIs
# ----------------------------
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Flights Displayed", len(filtered_df))
col2.metric("Airborne", int((filtered_df["on_ground"] == False).sum()))
col3.metric("Countries", filtered_df["origin_country"].nunique())
col4.metric("Max Altitude", round(filtered_df["baro_altitude_m"].max(skipna=True),1))
col5.metric("Min Altitude", round(filtered_df["baro_altitude_m"].min(skipna=True),1))

# ----------------------------
# Prepare marker size + color
# ----------------------------
filtered_df["baro_altitude_m_clean"] = filtered_df["baro_altitude_m"].fillna(0).clip(lower=0)
filtered_df["marker_size"] = filtered_df["baro_altitude_m_clean"] / 1000 + 5
filtered_df["color_altitude"] = pd.cut(
    filtered_df["baro_altitude_m_clean"],
    bins=[0, 3000, 6000, 9000, 12000, 15000, 20000],
    labels=["0-3k", "3k-6k", "6k-9k", "9k-12k", "12k-15k", "15k+"]
)

# ----------------------------
# Plotly world map
# ----------------------------
st.subheader("🌍 Flight Positions Map")

if not filtered_df.empty:
    fig_map = px.scatter_geo(
        filtered_df,
        lat="latitude",
        lon="longitude",
        hover_name="callsign",
        hover_data=["origin_country", "baro_altitude_m", "velocity_ms"],
        color="color_altitude",
        size="marker_size",
        projection="natural earth",
        height=600,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("No flights match the filters.")

# ----------------------------
# AI INSIGHTS — Outliers
# ----------------------------
st.subheader("🧠 AI Insights: Extreme Flights")

if not filtered_df.empty:
    # Remove NaNs
    ai_df = filtered_df.dropna(subset=["velocity_ms", "baro_altitude_m"])

    # Thresholds (99th percentile)
    speed_threshold = ai_df["velocity_ms"].quantile(0.99)
    altitude_threshold = ai_df["baro_altitude_m"].quantile(0.99)

    fastest = ai_df[ai_df["velocity_ms"] >= speed_threshold]
    highest = ai_df[ai_df["baro_altitude_m"] >= altitude_threshold]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🚀 Fastest Flights (Top 1%)")
        st.dataframe(
            fastest[["callsign", "origin_country", "velocity_ms", "longitude", "latitude"]]
            .sort_values("velocity_ms", ascending=False)
            .head(10),
            use_container_width=True
        )

    with col2:
        st.markdown("### 🛫 Highest Flights (Top 1%)")
        st.dataframe(
            highest[["callsign", "origin_country", "baro_altitude_m", "longitude", "latitude"]]
            .sort_values("baro_altitude_m", ascending=False)
            .head(10),
            use_container_width=True
        )

# ----------------------------
# AI INSIGHTS — Suspicious Flights
# ----------------------------
st.subheader("⚠️ AI Insights: Suspicious Flight Behavior")

suspicious = filtered_df[
    (filtered_df["on_ground"] == False) &
    (
        (filtered_df["velocity_ms"] < 30) |
        (filtered_df["baro_altitude_m"] < 0) |
        (filtered_df["latitude"].isna()) |
        (filtered_df["longitude"].isna())
    )
]

st.write(f"Detected **{len(suspicious)}** suspicious flights")

st.dataframe(
    suspicious[
        ["callsign", "origin_country", "velocity_ms", "baro_altitude_m", "latitude", "longitude"]
    ].head(20),
    use_container_width=True
)

# ----------------------------
# AI INSIGHTS — Country Intelligence
# ----------------------------
st.subheader("🌐 AI Insights: Country Intelligence")

country_summary = (
    filtered_df
    .groupby("origin_country")
    .agg(
        flights=("callsign", "count"),
        avg_speed=("velocity_ms", "mean"),
        avg_altitude=("baro_altitude_m", "mean")
    )
    .sort_values("flights", ascending=False)
    .head(10)
    .reset_index()
)

st.dataframe(country_summary, use_container_width=True)

fig_country = px.bar(
    country_summary,
    x="origin_country",
    y="flights",
    title="Top 10 Countries by Active Flights"
)
st.plotly_chart(fig_country, use_container_width=True)

# ----------------------------
# AI — Short-term trajectory prediction
# ----------------------------
EARTH_RADIUS = 6371000  # meters

def predict_position(lat, lon, speed_ms, heading_deg, delta_t):
    """
    Predict future lat/lon after delta_t seconds
    """
    if pd.isna(lat) or pd.isna(lon) or pd.isna(speed_ms) or pd.isna(heading_deg):
        return lat, lon

    heading_rad = np.deg2rad(heading_deg)
    distance = speed_ms * delta_t  # meters

    delta_lat = (distance * np.cos(heading_rad)) / EARTH_RADIUS
    delta_lon = (distance * np.sin(heading_rad)) / (EARTH_RADIUS * np.cos(np.deg2rad(lat)))

    new_lat = lat + np.rad2deg(delta_lat)
    new_lon = lon + np.rad2deg(delta_lon)

    return new_lat, new_lon

PREDICTION_SECONDS = 300  # 5 minutes

pred_df = filtered_df.copy()

pred_df[["pred_lat", "pred_lon"]] = pred_df.apply(
    lambda r: predict_position(
        r["latitude"],
        r["longitude"],
        r["velocity_ms"],
        r["true_track"],
        PREDICTION_SECONDS
    ),
    axis=1,
    result_type="expand"
)

# ----------------------------
# Visualize Actual vs Predicted (AI Visualization)
# ----------------------------

st.subheader("🔮 AI: Short-term Trajectory Prediction (5 min)")

fig_pred = px.scatter_geo(
    pred_df,
    lat="latitude",
    lon="longitude",
    color_discrete_sequence=["blue"],
    opacity=0.6,
    height=600,
)

fig_pred.add_scattergeo(
    lat=pred_df["pred_lat"],
    lon=pred_df["pred_lon"],
    mode="markers",
    marker=dict(color="red", size=4),
    name="Predicted Position"
)

st.plotly_chart(fig_pred, use_container_width=True)
# ----------------------------
# Predicted (AI Explanation)
#“We implemented short-term trajectory prediction 
# using a physics-based motion model derived from aircraft velocity and heading. 
# This avoids the need for historical training data while providing explainable, 
# near-real-time predictions suitable for aviation analytics.”
# ----------------------------







# import streamlit as st
# import pandas as pd
# from hdfs import InsecureClient
# import plotly.express as px

# # ----------------------------
# # Page setup
# # ----------------------------
# st.set_page_config(layout="wide", page_title="OpenSky Analytics")
# st.title("✈️ OpenSky Interactive Flight Map")
# st.caption("Live flights from Kafka → HDFS → Streamlit")

# status = st.empty()

# # ----------------------------
# # Safe HDFS loading function
# # ----------------------------
# @st.cache_data(ttl=60)
# def load_latest_processed_csv():
#     try:
#         client = InsecureClient("http://namenode:9870", user="root")
#         files = client.list("/opensky/processed")
#         csv_files = [f for f in files if f.endswith(".csv")]
#         if not csv_files:
#             return pd.DataFrame()
#         latest_file = sorted(csv_files)[-1]
#         with client.read(f"/opensky/processed/{latest_file}") as reader:
#             df = pd.read_csv(reader)
#         return df
#     except Exception as e:
#         st.error(f"HDFS connection failed: {e}")
#         return pd.DataFrame()

# # ----------------------------
# # Load data
# # ----------------------------
# with st.spinner("Loading latest flight data..."):
#     df = load_latest_processed_csv()

# if df.empty:
#     status.warning("No processed flight data available yet.")
#     st.stop()
# else:
#     status.success(f"Loaded {len(df)} flights from HDFS")

# # ----------------------------
# # Filters
# # ----------------------------
# st.sidebar.header("Filters")

# # Country filter
# countries = df["origin_country"].unique()
# selected_countries = st.sidebar.multiselect(
#     "Select Countries", countries, default=list(countries)
# )

# # On-ground filter
# airborne_only = st.sidebar.checkbox("Airborne Only", value=True)

# # Apply filters
# filtered_df = df[
#     (df["origin_country"].isin(selected_countries)) &
#     ((df["on_ground"] == False) if airborne_only else True)
# ]

# st.sidebar.write(f"Flights displayed: {len(filtered_df)}")

# # ----------------------------
# # Metrics
# # ----------------------------
# col1, col2, col3 = st.columns(3)
# col1.metric("Flights Displayed", len(filtered_df))
# col2.metric("Airborne", int((filtered_df["on_ground"] == False).sum()))
# col3.metric("Countries", filtered_df["origin_country"].nunique())

# # ----------------------------
# # Plotly world map
# # ----------------------------
# st.subheader("🌍 Flight Positions Map")

# if not filtered_df.empty:
#     # Clean altitude for plotting
#     filtered_df["baro_altitude_m_clean"] = filtered_df["baro_altitude_m"].fillna(0)
#     # Make sure all sizes are non-negative
#     filtered_df["baro_altitude_m_clean"] = filtered_df["baro_altitude_m_clean"].clip(lower=0)

#     # Optional: scale for better visibility
#     filtered_df["marker_size"] = filtered_df["baro_altitude_m_clean"] / 1000 + 5  # minimum size 5

#     fig_map = px.scatter_geo(
#         filtered_df,
#         lat="latitude",
#         lon="longitude",
#         hover_name="callsign",
#         hover_data=["origin_country", "baro_altitude_m", "velocity_ms"],
#         color="origin_country",
#         size="marker_size",
#         projection="natural earth",
#         height=600
#     )
#     st.plotly_chart(fig_map, use_container_width=True)
# else:
#     st.info("No flights match the filters.")


## ------------------------------

# import streamlit as st
# import pandas as pd
# from hdfs import InsecureClient
# import plotly.express as px

# # ----------------------------
# # Page setup
# # ----------------------------
# st.set_page_config(layout="wide", page_title="OpenSky Analytics")

# st.title("✈️ OpenSky Analytics")
# st.caption("Live data from Kafka → HDFS → Streamlit")

# status = st.empty()

# # ----------------------------
# # Safe HDFS loading function
# # ----------------------------
# @st.cache_data(ttl=60)
# def load_latest_processed_csv():
#     try:
#         client = InsecureClient("http://namenode:9870", user="root")
#         files = client.list("/opensky/processed")
#         csv_files = [f for f in files if f.endswith(".csv")]
#         if not csv_files:
#             return pd.DataFrame()
#         latest_file = sorted(csv_files)[-1]
#         with client.read(f"/opensky/processed/{latest_file}") as reader:
#             df = pd.read_csv(reader)
#         return df
#     except Exception as e:
#         st.error(f"HDFS connection failed: {e}")
#         return pd.DataFrame()

# # ----------------------------
# # Load data
# # ----------------------------
# with st.spinner("Loading latest processed flights..."):
#     df = load_latest_processed_csv()

# if df.empty:
#     status.warning("No processed data available yet.")
#     st.stop()
# else:
#     status.success(f"Loaded {len(df)} flights from HDFS")

# # ----------------------------
# # Show basic metrics
# # ----------------------------
# col1, col2, col3 = st.columns(3)

# col1.metric("Active Flights", len(df))
# col2.metric("Airborne", int((df["on_ground"] == False).sum()))
# col3.metric("Countries", df["origin_country"].nunique())

# # ----------------------------
# # Plotly chart — altitude distribution
# # ----------------------------
# st.subheader("📊 Altitude Distribution")

# fig = px.histogram(df, x="baro_altitude_m", nbins=40)
# st.plotly_chart(fig, use_container_width=True)

