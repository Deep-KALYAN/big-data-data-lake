import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import plotly.express as px
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import datetime

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
# Apply clustering to add flight phase labels
# ----------------------------
def add_phase_of_flight_clusters(df):
    """
    Adds an unsupervised phase-of-flight cluster label
    based on altitude, speed, and vertical rate.
    """

    required_cols = [
        "baro_altitude_m",
        "velocity_ms",
        "vertical_rate_ms"
    ]

    # Keep only valid rows
    features = df[required_cols].dropna()

    if len(features) < 50:
        df["flight_phase"] = "Unknown"
        return df

    # Normalize features (CRITICAL)
    scaler = StandardScaler()
    X = scaler.fit_transform(features)

    # KMeans clustering
    kmeans = KMeans(n_clusters=3, random_state=42)
    clusters = kmeans.fit_predict(X)

    # Assign cluster ids
    df.loc[features.index, "phase_cluster"] = clusters

    return df

# ----------------------------
# Label flight phases based on clusters
# ----------------------------
def label_flight_phases(df):
    phase_stats = df.groupby("phase_cluster")[[
        "baro_altitude_m",
        "velocity_ms",
        "vertical_rate_ms"
    ]].mean()

    phase_map = {}

    for cluster, row in phase_stats.iterrows():
        if row["vertical_rate_ms"] > 1:
            phase_map[cluster] = "Takeoff / Climb"
        elif row["vertical_rate_ms"] < -1:
            phase_map[cluster] = "Descent / Approach"
        else:
            phase_map[cluster] = "Cruise"

    df["flight_phase"] = df["phase_cluster"].map(phase_map)
    return df



# ----------------------------
# Load data
# ----------------------------
with st.spinner("Loading latest flight data..."):
    df = load_latest_processed_csv()
    df = add_phase_of_flight_clusters(df)
    df = label_flight_phases(df)

if df.empty:
    status.warning("No processed flight data available yet.")
    st.stop()
else:
    status.success(f"Loaded {len(df)} flights from HDFS")


# ----------------------------
# min max date of data
# ----------------------------
# Convert Unix timestamp to datetime objects
df['timestamp_dt'] = pd.to_datetime(df['time_position'], unit='s')

# Find the range
min_date = df['timestamp_dt'].min()
max_date = df['timestamp_dt'].max()


# Check if the latest data is older than 5 minutes
time_diff = datetime.datetime.now() - max_date

# Format the dates as strings once to keep the code clean
start_str = min_date.strftime('%Y-%m-%d %H:%M')
end_str = max_date.strftime('%Y-%m-%d %H:%M')

if time_diff.total_seconds() > 300:
    # Use an f-string to combine everything into ONE argument
    st.warning(f"⚠️ Data delayed ({int(time_diff.total_seconds()/60)}m ago) | Start: {start_str} | Last: {end_str}")
else:
    # Use an f-string here too
    st.success(f"✅ Real-Time Stream | Start: {start_str} | Last Update: {end_str}")


# ----------------------------
# Sidebar Filters
# ----------------------------
st.sidebar.header("Filters")

# # Origin country filter
# countries = df["origin_country"].unique()
# selected_countries = st.sidebar.multiselect(
#     "Aircraft Registration Country", countries, default=list(countries)
# )
# --- Origin country filter with Select All ---
countries = sorted(df["origin_country"].dropna().unique())

# Add the checkbox
select_all_countries = st.sidebar.checkbox("Select All Countries", value=True)

if select_all_countries:
    # If select all is ON, display the multiselect but pre-fill it and disable it
    selected_countries = st.sidebar.multiselect(
        "Aircraft Registration Country", countries, default=list(countries), disabled=True
    )
else:
    # If OFF, let the user pick
    selected_countries = st.sidebar.multiselect(
        "Aircraft Registration Country", countries, default=[]
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
# Filters
# ----------------------------
# st.sidebar.header("Filters")

# Flight phase filter
phase_options = df["flight_phase"].dropna().unique()
selected_phase = st.sidebar.multiselect(
    "Flight Phase",
    options=phase_options,
    default=list(phase_options)
)

# Get unique list of callsigns, sorted, and remove any empty values
all_callsigns = sorted(df["callsign"].dropna().unique())

# Add a Search/Filter for specific callsigns
selected_callsigns = st.sidebar.multiselect(
    "Search specific Flight(s)",
    options=all_callsigns,
    help="Type the callsign (e.g., IGO1151) to find a specific aircraft."
)

st.sidebar.subheader("Metadata Filters")
if not df.empty and 'operator' in df.columns:
    operators = sorted(df["operator"].dropna().unique())
    selected_operators = st.sidebar.multiselect(
        "Filter by Operator", operators, default=[]
    )
# ----------------------------
# Apply filters
# ----------------------------
# Create a mask for callsign filter: If nothing is selected, show all. 
# If something is selected, filter by those specific callsigns.
callsign_filter = df["callsign"].isin(selected_callsigns) if selected_callsigns else True
operator_filter = df["operator"].isin(selected_operators) if selected_operators else True
filtered_df = df[
    (df["origin_country"].isin(selected_countries)) &
    ((df["on_ground"] == False) if airborne_only else True) &
    (df["baro_altitude_m"].fillna(0).between(min_alt, max_alt)) &
    (df["latitude"].between(lat_range[0], lat_range[1])) &
    (df["longitude"].between(lon_range[0], lon_range[1])) &
    (df["flight_phase"].isin(selected_phase)) &
    (callsign_filter) &
    (operator_filter)
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
        height=800,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("No flights match the filters.")



# ----------------------------
# INSIGHTS — Outliers
# ----------------------------
st.subheader("🧠 Insights: Extreme Flights")

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
# INSIGHTS — Suspicious Flights
# ----------------------------
st.subheader("⚠️ Insights: Suspicious Flight Behavior")

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
# INSIGHTS — Country Intelligence
# ----------------------------
st.subheader("🌐 Insights: Country Intelligence")

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

if not pred_df.empty:
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
else:
    # Create empty columns if there is no data to prevent the dashboard from crashing later
    pred_df["pred_lat"] = None
    pred_df["pred_lon"] = None

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
    height=800,
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


st.subheader("📊 Flight KPIs")

col1, col2, col3 = st.columns(3)
col1.metric("Flights Displayed", len(filtered_df))
col2.metric("Airborne", (filtered_df["on_ground"] == False).sum())
col3.metric("Countries", filtered_df["origin_country"].nunique())



# Marker size safe (positive only)
marker_size = np.where(
    np.isnan(filtered_df["baro_altitude_m"]),
    5,  # default size if NaN
    np.clip(filtered_df["baro_altitude_m"] / 1000, 5, 20)  # scale
)

fig_map = px.scatter_geo(
    filtered_df,
    lat="latitude",
    lon="longitude",
    color="flight_phase",
    hover_name="callsign",
    hover_data={
        "origin_country": True,
        "velocity_ms": True,
        "baro_altitude_m": True,
        "vertical_rate_ms": True
    },
    size=marker_size,
    projection="natural earth",
    title="🌍 Flight Positions Map by Phase",
    height=800
)

# Streamlit display
st.plotly_chart(fig_map, use_container_width=True)


st.subheader("🧪 Phase-of-Flight Summary")
st.dataframe(
    df.groupby("flight_phase")[[
        "baro_altitude_m",
        "velocity_ms",
        "vertical_rate_ms"
    ]].mean().round(2)
)

# ----------------------------
# METADATA KPIS & ENRICHMENT INSIGHTS
# ----------------------------
st.markdown("---")
st.subheader("🏢 Fleet & Manufacturer Intelligence")

if not filtered_df.empty:
    m_col1, m_col2, m_col3 = st.columns(3)
    
    with m_col1:
        # Safety check for mode
        m_mode = filtered_df['manufacturerName'].dropna().mode()
        top_manuf = m_mode.iloc[0] if not m_mode.empty else "N/A"
        st.metric("Top Manufacturer", top_manuf)
    
    with m_col2:
        # Unique models count
        unique_models = filtered_df['model'].nunique()
        st.metric("Unique Models", unique_models)
        
    with m_col3:
        # Most frequent category
        c_mode = filtered_df['categoryDescription'].dropna().mode()
        top_cat = c_mode.iloc[0] if not c_mode.empty else "N/A"
        st.metric("Primary Category", top_cat)

    # Visualization: Top Manufacturers Chart
    st.markdown("### 📊 Active Fleet by Manufacturer")
    manuf_counts = filtered_df['manufacturerName'].value_counts().head(10).reset_index()
    
    if not manuf_counts.empty:
        manuf_counts.columns = ['Manufacturer', 'Count']
        fig_manuf = px.bar(
            manuf_counts, 
            x='Count', 
            y='Manufacturer', 
            orientation='h',
            color='Count',
            template="plotly_dark"
        )
        st.plotly_chart(fig_manuf, use_container_width=True)
    else:
        st.info("No manufacturer data available for current filters.")

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

