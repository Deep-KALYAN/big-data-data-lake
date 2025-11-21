🌍 OpenSky Real-Time DataLake Pipeline

A complete end-to-end Big Data architecture using Kafka, Hadoop HDFS, Python, Docker, and Streamlit.

----------------------------------------------------
📘 Table of Contents
----------------------------------------------------

Overview

Architecture

Data Flow

Tech Stack

Project Structure

How to Run

Service Details

Dashboard

Testing

Troubleshooting

Contributors

----------------------------------------------------
🌐 Overview
----------------------------------------------------

This project implements a production-style DataLake architecture for real-time aircraft tracking data using the OpenSky API.

It follows enterprise Big Data concepts:

✔ Ingestion Layer: Python Producer → Kafka
✔ Persistence Layer: HDFS (raw + processed zones)
✔ Processing Layer: Kafka Consumer performing ETL
✔ Analytics Layer: Streamlit dashboard reading HDFS

The system is fully containerized using Docker Compose.

----------------------------------------------------
🏛 Architecture
----------------------------------------------------
                  ┌────────────────────────────┐
                  │        OpenSky API         │
                  └───────────────┬────────────┘
                                  │ (Batch Pull)
                                  ▼
                      ┌─────────────────────┐
                      │  Producer (Python)  │
                      └───────────┬─────────┘
                                  │
                                  ▼
   ┌──────────────────────────────────────────────────────────┐
   │                        Kafka Cluster                      │
   │     ┌──────────────┐       ┌─────────────────────────┐   │
   │     │   Zookeeper   │◄────►│     Kafka Broker        │   │
   │     └──────────────┘       │ Topics: opensky, raw    │   │
   └───────────┬────────────────┴──────────────────────────┘
               │
               ▼
      ┌─────────────────────────┐
      │   Consumer (Python)     │
      └───────┬────────┬────────┘
              │        │
              │        │ Processed data
              │        ▼
              │   /opensky/processed
              │
              ▼ Raw data
      /opensky/raw
   ┌─────────────────────────────┐
   │          HDFS               │
   │ Namenode + Datanode         │
   └─────────────────────────────┘
              │
              ▼
  ┌──────────────────────────────┐
  │     Streamlit Dashboard      │
  └──────────────────────────────┘

----------------------------------------------------
🔁 Data Flow
----------------------------------------------------
1️⃣ Producer → Kafka

Fetches aircraft data from OpenSky REST API

Sends raw JSON to Kafka topic opensky

2️⃣ Kafka → Consumer → HDFS

Consumer listens to Kafka topic

Saves raw data in /opensky/raw

Cleans & transforms data (ETL)

Saves processed data in /opensky/processed

3️⃣ Streamlit Dashboard

Reads processed data from HDFS

Displays metrics, graphs, maps, and filters

----------------------------------------------------
🛠 Tech Stack
----------------------------------------------------
Layer	Technology	Purpose
Ingestion	Python, Requests	Fetch OpenSky API
Messaging	Kafka + Zookeeper	Streaming pipeline
Storage	HDFS (Hadoop 3.2.1)	DataLake zones
Processing	Python Consumer	ETL (raw → processed)
Analytics	Streamlit	Interactive dashboard
Orchestration	Docker Compose	Service orchestration
----------------------------------------------------
📁 Project Structure
----------------------------------------------------
FINAL_PROJECT/
│
├── docker-compose.yml              # Orchestration
├── hadoop.env                      # Hadoop config
├── requirements.txt                # Python libs
│
├── services/
│   ├── producer/
│   │   ├── producer.py
│   │   └── Dockerfile
│   ├── consumer/
│   │   ├── consumer.py
│   │   └── Dockerfile
│   └── dashboard/
│       ├── dashboard.py
│       └── Dockerfile
│
├── myhadoop/                       # HDFS local mount
│
├── test_hdfs.py
├── test_kafka.py
└── test_imports.py

----------------------------------------------------
▶️ How to Run the Project
----------------------------------------------------
0. Install prerequisites

Docker

Docker Compose

Python 3.10+ (optional for local tests)

1. Create Docker network
docker network create sky-net

2. Start the complete system
docker-compose up --build -d

3. Check running containers
docker ps


Expected services:

Service	Status
namenode	Running
datanode	Running
kafka	Running
zookeeper	Running
opensky-producer	Running
opensky-consumer	Running
opensky-dashboard	Running
4. Access important UIs
Component	URL
Dashboard	http://localhost:8501

HDFS Namenode UI	http://localhost:9870

Kafka	9092
Zookeeper	2181
----------------------------------------------------
📦 Service Details
----------------------------------------------------
🛫 Producer (OpenSky → Kafka)

Path: services/producer/producer.py

Calls OpenSky API every X seconds

Parses JSON payload

Sends message to Kafka topic opensky

🔄 Consumer (Kafka → HDFS)

Path: services/consumer/consumer.py

Reads Kafka messages

Writes raw JSON → /opensky/raw

Cleaned + transformed data → /opensky/processed

Uses WebHDFS API

🧱 HDFS (Hadoop DataLake)

Directories:

/opensky/raw
/opensky/processed


Namenode UI: http://localhost:9870

📊 Streamlit Dashboard

Path: services/dashboard/dashboard.py

Features:

✔ Explore raw or processed data
✔ Aircraft statistics
✔ Search + filtering
✔ Real-time update button
✔ Data preview tables
✔ Graphs + charts

----------------------------------------------------
📈 Dashboard
----------------------------------------------------

Open the dashboard:

👉 http://localhost:8501

Shows:

Number of active flights

Altitude distribution

Country-based filtering

Map (optional)

Raw vs processed data quality

Custom analytics