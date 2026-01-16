import os
import time
import pandas as pd
from hdfs import InsecureClient
from io import BytesIO
import json

# Configuration
HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
BRONZE_DIR = '/opensky/raw'
SILVER_DIR = '/opensky/silver'
METADATA_PATH = '/app/metadata/aircraft_db.csv'
STATE_FILE = 'last_processed_file.txt'

client = InsecureClient(HDFS_URL, user=HDFS_USER)

def get_last_processed():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return f.read().strip()
    return ""

def save_last_processed(filename):
    with open(STATE_FILE, 'w') as f:
        f.write(filename)

def process_state(state_array):
    """Convert OpenSky state array into dict."""
    if len(state_array) >= 17:
        return {
            "icao24": state_array[0],
            "callsign": state_array[1].strip() if state_array[1] else None,
            "origin_country": state_array[2],
            "time_position": state_array[3],
            "last_contact": state_array[4],
            "longitude": state_array[5],
            "latitude": state_array[6],
            "baro_altitude_m": state_array[7],
            "on_ground": state_array[8],
            "velocity_ms": state_array[9],
            "true_track": state_array[10],
            "vertical_rate_ms": state_array[11],
            "geo_altitude_m": state_array[13] if len(state_array) > 13 else None,
            "squawk": state_array[14] if len(state_array) > 14 else None,
            "spi": state_array[15] if len(state_array) > 15 else None,
            "position_source": state_array[16],
            "category": state_array[17] if len(state_array) > 17 else None
        }
    return {}

def run_enrichment_job(metadata_df):
    last_file = get_last_processed()
    
    try:
        all_files = sorted([f for f in client.list(BRONZE_DIR) if f.endswith('.jsonl')])
    except Exception as e:
        print(f"⚠️ Could not list files: {e}")
        return

    new_files = [f for f in all_files if f > last_file]
    
    if not new_files:
        print("⏳ No new files")
        return
    
    print(f"💎 Found {len(new_files)} new files")
    
    for file_name in new_files:  # Process only first file for testing
        try:
            print(f"📂 Processing {file_name}")
            
            # Read raw data
            with client.read(f"{BRONZE_DIR}/{file_name}") as reader:
                content = reader.read()
                
                # DEBUG: Show first record
                lines = content.decode('utf-8').strip().split('\n')
                if lines:
                    first_record = json.loads(lines[0])
                    print(f"   First record type: {type(first_record)}")
                    print(f"   First record: {first_record}")
                
                # Read JSONL - but OpenSky data comes as arrays, not objects
                # We need to convert array format to proper columns
                data_list = []
                for line in lines:
                    record = json.loads(line)
                    data_list.append(record)
                
                # OpenSky State Vector format (17 elements)
                column_names = [
                    'icao24', 'callsign', 'origin_country', 'time_position', 
                    'last_contact', 'longitude', 'latitude', 'baro_altitude_m', 
                    'on_ground', 'velocity_ms', 'true_track', 'vertical_rate_ms',
                    'sensors', 'geo_altitude_m', 'squawk', 'spi', 'position_source'
                ]
                
                # Convert list of arrays to DataFrame
                df_raw = pd.DataFrame(data_list, columns=column_names[:len(data_list[0])])
            
            print(f"   Raw shape: {df_raw.shape}")
            print(f"   Raw columns: {list(df_raw.columns)}")
            print(f"   First few icao24 values: {df_raw['icao24'].head().tolist()}")
            
            # Clean icao24
            df_raw['icao24'] = df_raw['icao24'].astype(str).str.lower().str.strip()
            
            # Merge with metadata
            print(f"   Merging with metadata ({len(metadata_df)} records)")
            df_enriched = pd.merge(df_raw, metadata_df, on='icao24', how='left')
            print(f"   Enriched shape: {df_enriched.shape}")
            
            # Save as Parquet
            parquet_name = file_name.replace('.jsonl', '.parquet')
            hdfs_silver_path = f"{SILVER_DIR}/{parquet_name}"
            
            buffer = BytesIO()
            df_enriched.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            client.write(hdfs_silver_path, buffer.getvalue(), overwrite=True)
            
            save_last_processed(file_name)
            print(f"✅ Saved: {parquet_name}")
            print(f"   Records: {len(df_enriched)}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            break
# def run_enrichment_job(metadata_df):
#     last_file = get_last_processed()
    
#     try:
#         all_files = sorted([f for f in client.list(BRONZE_DIR) if f.endswith('.jsonl')])
#     except Exception as e:
#         print(f"⚠️ Could not list files: {e}")
#         return

#     new_files = [f for f in all_files if f > last_file]
    
#     if not new_files:
#         print("⏳ No new files")
#         return
    
#     print(f"💎 Found {len(new_files)} new files")
    
#     for file_name in new_files[:1]:  # Process only first file for testing
#         try:
#             print(f"📂 Processing {file_name}")
            
#             # Read raw data
#             with client.read(f"{BRONZE_DIR}/{file_name}") as reader:
#                 content = reader.read()
#                 df_raw = pd.read_json(BytesIO(content), lines=True)
            
#             print(f"   Raw shape: {df_raw.shape}")
#             print(f"   Raw columns: {list(df_raw.columns)}")
            
#             # Check and rename icao24 column if needed
#             icao_col = None
#             for col in df_raw.columns:
#                 if 'icao' in col.lower():
#                     icao_col = col
#                     break
            
#             if not icao_col:
#                 print("❌ No ICAO column found in raw data!")
#                 break
            
#             if icao_col != 'icao24':
#                 df_raw = df_raw.rename(columns={icao_col: 'icao24'})
#                 print(f"   Renamed {icao_col} → icao24")
            
#             # Clean icao24
#             df_raw['icao24'] = df_raw['icao24'].astype(str).str.lower().str.strip()
            
#             # Merge with metadata
#             print(f"   Merging with metadata ({len(metadata_df)} records)")
#             df_enriched = pd.merge(df_raw, metadata_df, on='icao24', how='left')
#             print(f"   Enriched shape: {df_enriched.shape}")
            
#             # Save as Parquet
#             parquet_name = file_name.replace('.jsonl', '.parquet')
#             hdfs_silver_path = f"{SILVER_DIR}/{parquet_name}"
            
#             buffer = BytesIO()
#             df_enriched.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
#             client.write(hdfs_silver_path, buffer.getvalue(), overwrite=True)
            
#             save_last_processed(file_name)
#             print(f"✅ Saved: {parquet_name}")
            
#         except Exception as e:
#             print(f"❌ Error: {e}")
#             import traceback
#             traceback.print_exc()
#             break

if __name__ == "__main__":
    print("📖 Loading aircraft metadata...")
    
    try:
        # DEBUG: First let's test reading the CSV
        print("=== DEBUG: Testing CSV reading ===")
        
        # Test 1: Read without any parameters
        test_df = pd.read_csv(METADATA_PATH, nrows=5)
        print(f"Test 1 - Default read:")
        print(f"  Shape: {test_df.shape}")
        print(f"  Columns: {list(test_df.columns)}")
        print(f"  First row: {test_df.iloc[0].tolist()[:5] if len(test_df) > 0 else 'No data'}")
        print()
        
        # Test 2: Read with quotechar
        test_df2 = pd.read_csv(METADATA_PATH, quotechar="'", nrows=5)
        print(f"Test 2 - With quotechar=\"'\":")
        print(f"  Shape: {test_df2.shape}")
        print(f"  Columns: {list(test_df2.columns)}")
        print()
        
        # Test 3: Read with engine='python' (more forgiving)
        test_df3 = pd.read_csv(METADATA_PATH, quotechar="'", engine='python', nrows=5)
        print(f"Test 3 - With engine='python':")
        print(f"  Shape: {test_df3.shape}")
        print(f"  Columns: {list(test_df3.columns)}")
        print(f"  First few rows:")
        print(test_df3.head())
        print("="*50)
        
        # Now read the full dataset with the correct parameters
        print("\n   Reading full metadata...")
        meta_df = pd.read_csv(
            METADATA_PATH,
            quotechar="'",  # Single quotes around fields
            engine='python',  # More forgiving parser
            usecols=['icao24', 'model', 'operator', 'manufacturerName', 'categoryDescription']
        )
        
        print(f"   ✅ Full metadata loaded: {len(meta_df)} records")
        print(f"   Columns: {list(meta_df.columns)}")
        
        # Clean icao24
        meta_df['icao24'] = meta_df['icao24'].astype(str).str.lower().str.strip()
        
        # Fill NaN values
        for col in ['model', 'operator', 'manufacturerName', 'categoryDescription']:
            meta_df[col] = meta_df[col].fillna('Unknown')
        
        print(f"   Sample data (first 3):")
        print(meta_df.head(3))
        
        # Ensure Silver directory exists
        if not client.content(SILVER_DIR, strict=False):
            client.makedirs(SILVER_DIR)
            print(f"📁 Created {SILVER_DIR}")
        
        print("🚀 Silver Layer Service Running...")
        while True:
            run_enrichment_job(meta_df)
            time.sleep(30)
            
    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

# import os
# import time
# import pandas as pd
# from hdfs import InsecureClient
# from io import BytesIO, StringIO

# # Configuration
# HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
# HDFS_USER = os.getenv('HDFS_USER', 'root')
# BRONZE_DIR = '/opensky/raw'
# SILVER_DIR = '/opensky/silver'
# METADATA_PATH = '/app/metadata/aircraft_db.csv' # Path in container
# STATE_FILE = 'last_processed_file.txt'

# client = InsecureClient(HDFS_URL, user=HDFS_USER)

# def get_last_processed():
#     if os.path.exists(STATE_FILE):
#         with open(STATE_FILE, 'r') as f:
#             return f.read().strip()
#     return ""

# def save_last_processed(filename):
#     with open(STATE_FILE, 'w') as f:
#         f.write(filename)

# def run_enrichment_job(metadata_df):
#     last_file = get_last_processed()
    
#     # 1. List files in HDFS Bronze
#     try:
#         all_files = sorted([f for f in client.list(BRONZE_DIR) if f.endswith('.jsonl')])
#     except Exception:
#         return # Directory might not exist yet

#     new_files = [f for f in all_files if f > last_file]

#     if not new_files:
#         return

#     print(f"💎 Found {len(new_files)} new raw files. Enriching...")

#     for file_name in new_files:
#         try:
#             # 2. Read JSONL from HDFS
#             with client.read(f"{BRONZE_DIR}/{file_name}") as reader:
#                 # OpenSky raw is JSONL (one JSON object per line)
#                 df_raw = pd.read_json(BytesIO(reader.read()), lines=True)

#             # 3. Join with Metadata (icao24 is the key)
#             # Ensure icao24 is string and stripped
#             df_raw['icao24'] = df_raw['icao24'].astype(str).str.lower().str.strip()
#             df_enriched = pd.merge(df_raw, metadata_df, on='icao24', how='left')

#             # 4. Cleanup
#             df_enriched['baro_altitude_m'] = df_enriched['baro_altitude_m'].fillna(0)
#             df_enriched['timestamp_dt'] = pd.to_datetime(df_enriched['time_position'], unit='s')

#             # 5. Write to HDFS as Parquet
#             parquet_name = file_name.replace('.jsonl', '.parquet')
#             hdfs_silver_path = f"{SILVER_DIR}/{parquet_name}"
            
#             # Use BytesIO to buffer parquet before sending to HDFS
#             buffer = BytesIO()
#             df_enriched.to_parquet(buffer, engine='pyarrow', compression='snappy')
#             client.write(hdfs_silver_path, buffer.getvalue(), overwrite=True)

#             # 6. Update Checkpoint
#             save_last_processed(file_name)
#             print(f"✅ Enriched & Saved: {parquet_name}")

#         except Exception as e:
#             print(f"❌ Error processing {file_name}: {e}")
#             break

# if __name__ == "__main__":
#     # Load Metadata into memory once
#     print("📖 Loading aircraft metadata...")
#     # Adjust columns based on your specific CSV structure
#     meta_df = pd.read_csv(METADATA_PATH, usecols=["'icao24'", "'model'", "'operator'", "'manufacturerName'", "'owner'", "'categoryDescription'"])
#     meta_df["'icao24'"] = meta_df["'icao24'"].astype(str).str.lower().str.strip()
    
#     # Ensure Silver Dir exists in HDFS
#     if not client.content(SILVER_DIR, False):
#         client.makedirs(SILVER_DIR)

#     print("🚀 Silver Layer Service Running...")
#     while True:
#         run_enrichment_job(meta_df)
#         time.sleep(30)