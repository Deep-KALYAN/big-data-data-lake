import json
import time
import os
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'opensky'

# HDFS configuration
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'

HDFS_RAW_DIR = '/opensky/raw'
HDFS_PROCESSED_DIR = '/opensky/processed'
METADATA_PATH = '/app/metadata/aircraft_db.csv'

# Batching settings
FLUSH_INTERVAL = 60  # seconds

# Connect to HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER)

#---------------------------------------
# Load metadata once at startup
print("📖 Loading aircraft metadata...")
try:
    metadata_df = pd.read_csv(
        METADATA_PATH,
        quotechar="'",  # Your CSV has single quotes
        usecols=['icao24', 'model', 'operator', 'manufacturerName', 'categoryDescription']
    )
    # Clean icao24 for matching
    metadata_df['icao24'] = metadata_df['icao24'].astype(str).str.lower().str.strip()
    print(f"✅ Metadata loaded: {len(metadata_df)} records")
except Exception as e:
    print(f"❌ Failed to load metadata: {e}")
    metadata_df = pd.DataFrame(columns=['icao24', 'model', 'operator', 'manufacturerName', 'categoryDescription'])
#---------------------------------------

# Ensure directories exist
for dir_path in [HDFS_RAW_DIR, HDFS_PROCESSED_DIR]:
    try:
        client.status(dir_path)
    except:
        client.makedirs(dir_path)

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    group_id='opensky-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🚀 Consumer started with batching enabled...")

raw_buffer = []
processed_buffer = []
last_flush = time.time()

def process_state(s, metadata_df):
    """Convert OpenSky state array into dict (from your original code)and enrich with metadata."""
    basic_info = {
        "icao24": s[0],
        "callsign": s[1].strip() if s[1] else None,
        "origin_country": s[2],
        "time_position": s[3],
        "last_contact": s[4],
        "longitude": s[5],
        "latitude": s[6],
        "baro_altitude_m": s[7],
        "on_ground": s[8],
        "velocity_ms": s[9],
        "true_track": s[10],
        "vertical_rate_ms": s[11],
        "geo_altitude_m": s[13] if len(s) > 13 else None,
        "squawk": s[14] if len(s) > 14 else None,
        "spi": s[15] if len(s) > 15 else None,
        "position_source": s[16],
        "category": s[17] if len(s) > 17 else None
    }
    # Enrich with metadata
    icao_key = str(s[0]).lower().strip() if s[0] else ""
    if icao_key and not metadata_df.empty:
        # Find matching metadata
        match = metadata_df[metadata_df['icao24'] == icao_key]
        if not match.empty:
            basic_info.update({
                "model": match.iloc[0].get('model'),
                "operator": match.iloc[0].get('operator'),
                "manufacturerName": match.iloc[0].get('manufacturerName'),
                "categoryDescription": match.iloc[0].get('categoryDescription')
            })
    
    return basic_info

def flush_to_hdfs():
    print("📖 Loading aircraft metadata...")
    try:
        metadata_df = pd.read_csv(
            METADATA_PATH,
            quotechar="'",
            usecols=['icao24', 'model', 'operator', 'manufacturerName', 'categoryDescription']
        )
        metadata_df['icao24'] = metadata_df['icao24'].astype(str).str.lower().str.strip()
        print(f"✅ Metadata loaded: {len(metadata_df)} records")
    except Exception as e:
        print(f"❌ Failed: {e}")
        metadata_df = pd.DataFrame()

    global raw_buffer, processed_buffer

    if not raw_buffer and not processed_buffer:
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # --- RAW: save JSON Lines ---
    if raw_buffer:
        raw_path = f"{HDFS_RAW_DIR}/raw_{timestamp}.jsonl"
        with client.write(raw_path, encoding="utf-8") as writer:
            for record in raw_buffer:
                writer.write(json.dumps(record) + "\n")
        print(f"📦 Saved RAW batch → {raw_path} ({len(raw_buffer)} records)")
        raw_buffer = []

    # --- PROCESSED: save CSV ---
    if processed_buffer:
        df = pd.DataFrame(processed_buffer)
        processed_path = f"{HDFS_PROCESSED_DIR}/processed_{timestamp}.csv"
        with client.write(processed_path, encoding="utf-8") as writer:
            df.to_csv(writer, index=False)
        print(f"📦 Saved PROCESSED batch → {processed_path} ({len(processed_buffer)} records)")
        processed_buffer = []

    print("📖 Loading aircraft metadata...")
    try:
        metadata_df = pd.read_csv(
            METADATA_PATH,
            quotechar="'",
            usecols=['icao24', 'model', 'operator', 'manufacturerName', 'categoryDescription']
        )
        metadata_df['icao24'] = metadata_df['icao24'].astype(str).str.lower().str.strip()
        print(f"✅ Metadata loaded: {len(metadata_df)} records")
    except Exception as e:
        print(f"❌ Failed: {e}")
        metadata_df = pd.DataFrame()


# 🌀 Main loop
for message in consumer:
    data = message.value
    raw_buffer.append(data)  # Save raw message EXACTLY as received

    # ---- Detect state format ----
    if isinstance(data, dict):
        states = data.get("states", [])
    elif isinstance(data, list):
        states = data
        # Single-aircraft flat record?
        if states and isinstance(states[0], (str, float, int, bool, type(None))):
            states = [states]
    else:
        continue

    # ---- Process and clean ----
    for s in states:
        if isinstance(s, list) and len(s) >= 17:
            processed_buffer.append(process_state(s, metadata_df))

    # ---- Time-based flush ----
    if time.time() - last_flush > FLUSH_INTERVAL:
        flush_to_hdfs()
        last_flush = time.time()


# import json
# import time
# import pandas as pd
# from datetime import datetime
# from kafka import KafkaConsumer
# from hdfs import InsecureClient

# # Kafka configuration
# KAFKA_BROKER = 'kafka:9092' #'localhost:9092'
# TOPIC = 'opensky'

# # HDFS configuration
# HDFS_URL = 'http://namenode:9870'  # Container name of NameNode'http://localhost:50070'
# #'http://namenode:50070'  # Use container name "namenode" 'http://localhost:9870'  # NameNode web UI port
# HDFS_USER = 'root'
# HDFS_RAW_DIR = '/opensky/raw'
# HDFS_PROCESSED_DIR = '/opensky/processed'

# # Connect to HDFS
# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# BATCH_INTERVAL = 60       # 1 minute
# buffer = []

# # Ensure directories exist
# for dir_path in [HDFS_RAW_DIR, HDFS_PROCESSED_DIR]:
#     try:
#         client.status(dir_path)
#     except Exception:
#         client.makedirs(dir_path)

# # Create Kafka consumer
# consumer = KafkaConsumer(
#     TOPIC,
#     bootstrap_servers=KAFKA_BROKER,
#     auto_offset_reset='earliest',  #'latest',
#     group_id='opensky-consumer-group',
#     value_deserializer=lambda v: json.loads(v.decode('utf-8'))
# )

# print("🚀 Consumer started. Listening to Kafka...")

# last_flush = time.time()

# for message in consumer:
#     data = message.value
#     timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')

#     # --- Store raw data ---
#     raw_path = f"{HDFS_RAW_DIR}/opensky_raw_{timestamp}.json"
#     with client.write(raw_path, encoding='utf-8') as writer:
#         json.dump(data, writer)
#     print(f"✅ Raw data saved to HDFS: {raw_path}")

#     # --- Processed data (example: only callsign, lat, lon, velocity) ---
#     if isinstance(data, dict):
#         states = data.get('states', [])
#         print("ℹ️ Processing states from dict format")
#     elif isinstance(data, list):
#         states = data   # The list itself is the states data
#         print("ℹ️ Processing states from list format", states)
#         if states and isinstance(states[0], (str, float, int, bool, type(None))):
#             print("ℹ️ Detected flat single aircraft. Wrapping into list of lists.")
#             states = [states]
#     else:
#         print("⚠️ Unrecognized data format, skipping processing.")
#         states = []

#     processed = []
#     for s in states:
#         # Skip invalid rows
#         if not isinstance(s, list) or len(s) < 17:
#             print("⚠️ Invalid row skipped:", s)  # optional debug
#             continue

#         aircraft = {
#             "icao24": s[0],
#             "callsign": s[1].strip() if s[1] else None,
#             "origin_country": s[2],

#             # Time (keep UTC, convert later in analytics)
#             "time_position": s[3],
#             "last_contact": s[4],

#             # Geo data
#             "longitude": s[5],
#             "latitude": s[6],
#             "baro_altitude_m": s[7],          # meters
#             "geo_altitude_m": s[13] if len(s) > 13 else None,   # meters

#             # Flight state and movement
#             "on_ground": s[8],
#             "velocity_ms": s[9],             # m/s
#             "true_track": s[10],             # degrees
#             "vertical_rate_ms": s[11],       # m/s

#             # Other metadata
#             "squawk": s[14] if len(s) > 14 else None,
#             "spi": s[15] if len(s) > 15 else None,
#             "position_source": s[16],        # 0=ADS-B,1=ASTERIX,2=MLAT,3=FLARM

#             # Category may not exist → set None
#             "category": s[17] if len(s) > 17 else None
#         }

#         processed.append(aircraft)

    
#     if not processed:
#         print("⚠️ No valid aircraft data in this message, skipping...")
#         continue

#     df = pd.DataFrame(processed)


#     # Save processed CSV to HDFS
#     processed_path = f"{HDFS_PROCESSED_DIR}/opensky_processed_{timestamp}.csv"
#     with client.write(processed_path, encoding='utf-8') as writer:
#         df.to_csv(writer, index=False)
#     print(f"✅ Processed data saved to HDFS: {processed_path}")

