import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
 
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'opensky'
 
# OpenSky API URL
OPENSKY_URL = "https://opensky-network.org/api/states/all"
 
# Tor SOCKS5 proxy (dperson/torproxy)
TOR_PROXIES = {
    "http":  "socks5h://tor:9050",
    "https": "socks5h://tor:9050"
}
 
# 10 minutes TTL for aircraft data
TTL_SECONDS = 600
 
# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=5_000_000  # optional, just in case
)
 
logger.info("Kafka producer created successfully")
print("🚀 Producer started. Sending incremental data to Kafka via Tor...")
 
# # Kafka producer
# producer = None
# while producer is None:
#     try:
#         producer = KafkaProducer(
#             bootstrap_servers=KAFKA_BROKER,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             max_request_size=5_000_000  # optional, just in case
#         )
#         logger.info("Kafka producer created successfully")
#     except Exception as e:
#         logger.warning(f"Kafka not ready, retrying in 5 seconds: {e}")
#         time.sleep(5)
# print("🚀 Producer started. Sending incremental data to Kafka via Tor...")
 
 
# Track last seen for each aircraft
last_seen = {}
 
while True:
    try:
        def get_tor_exit_ip():
            r = requests.get(
                "https://api.ipify.org?format=json",
                proxies=TOR_PROXIES,
                timeout=10
            )
            return r.json()["ip"]
 
        exit_ip = get_tor_exit_ip()
        logger.info(f"Using Tor exit IP: {exit_ip}")
        # Fetch OpenSky data via Tor
        response = requests.get(OPENSKY_URL, proxies=TOR_PROXIES, timeout=20)
 
        if response.status_code != 200:
            logger.warning(f"OpenSky returned {response.status_code}")
            time.sleep(110)
            continue
 
        data = response.json()
        states = data.get("states", [])
 
        print("Fetched:", len(states), "flights")
        now = time.time()
 
        # Cleanup old aircraft (memory protection)
        stale = [icao for icao, ts in last_seen.items() if now - ts > TTL_SECONDS]
        for icao in stale:
            del last_seen[icao]
 
        new_count = 0
        for state in states:
            icao24 = state[0]      
            last_contact = state[4]
 
            if icao24 not in last_seen or last_contact > last_seen[icao24]:
                producer.send(TOPIC, state)
                last_seen[icao24] = last_contact
                new_count += 1
 
        producer.flush()
        print(f"✅ Sent {new_count} new/updated flights to Kafka")
        logger.info(f"✅ Sent {new_count} new/updated flights to Kafka")
 
    except Exception as e:
        logger.error(f"Error fetching/sending data: {e}")
 
    time.sleep(190)


# import json
# import time
# import requests
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# import logging

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Kafka configuration
# KAFKA_BROKER = 'kafka:9092'
# TOPIC = 'opensky'

# # OpenSky API URL
# OPENSKY_URL = "https://opensky-network.org/api/states/all"

# # Tor SOCKS5 proxy (dperson/torproxy)
# TOR_PROXIES = {
#     "http":  "socks5h://tor:9050",
#     "https": "socks5h://tor:9050"
# }

# # 10 minutes TTL for aircraft data
# TTL_SECONDS = 600

# # Kafka producer
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     max_request_size=5_000_000  # optional, just in case
# )

# logger.info("Kafka producer created successfully")
# print("🚀 Producer started. Sending incremental data to Kafka via Tor...")

# # # Kafka producer
# # producer = None
# # while producer is None:
# #     try:
# #         producer = KafkaProducer(
# #             bootstrap_servers=KAFKA_BROKER,
# #             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
# #             max_request_size=5_000_000  # optional, just in case
# #         )
# #         logger.info("Kafka producer created successfully")
# #     except Exception as e:
# #         logger.warning(f"Kafka not ready, retrying in 5 seconds: {e}")
# #         time.sleep(5)
# # print("🚀 Producer started. Sending incremental data to Kafka via Tor...")


# # Track last seen for each aircraft
# last_seen = {}  # {icao24: last_contact}

# while True:
#     try:
#         # Fetch OpenSky data via Tor
#         response = requests.get(OPENSKY_URL, proxies=TOR_PROXIES, timeout=20)

#         if response.status_code != 200:
#             logger.warning(f"OpenSky returned {response.status_code}")
#             time.sleep(10)
#             continue

#         data = response.json()
#         states = data.get("states", [])

#         print("Fetched:", len(states), "flights")
#         now = time.time()

#         # Cleanup old aircraft (memory protection)
#         stale = [icao for icao, ts in last_seen.items() if now - ts > TTL_SECONDS]
#         for icao in stale:
#             del last_seen[icao]

#         new_count = 0
#         for state in states:
#             icao24 = state[0]       
#             last_contact = state[4] 

#             if icao24 not in last_seen or last_contact > last_seen[icao24]:
#                 producer.send(TOPIC, state)
#                 last_seen[icao24] = last_contact
#                 new_count += 1

#         producer.flush()
#         print(f"✅ Sent {new_count} new/updated flights to Kafka")

#     except Exception as e:
#         logger.error(f"Error fetching/sending data: {e}")

#     time.sleep(90)
