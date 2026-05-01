# Real-Time Uber Ride Streaming Data Pipeline with Kafka & CI/CD

## Project Overview

This project is an end-to-end real-time data engineering pipeline built using an Uber-style ride booking simulation. The system generates ride booking events from a simulated application, streams those events through Azure Event Hubs using Kafka-compatible ingestion, processes the data in Azure Databricks, and stores curated datasets in Delta Lake using a Bronze, Silver, and Gold architecture.

The project combines both batch and streaming data processing. Historical ride data and reference lookup files are ingested using Azure Data Factory, while live ride booking events are streamed through Azure Event Hubs. Databricks is used to process, clean, validate, enrich, and model the data into analytics-ready fact and dimension tables.

The final Gold layer is designed using a star schema to support reporting and analytics. The project also includes a CI/CD approach using GitHub Actions to validate code and support deployment readiness.

---

## Architecture

![Real-Time Streaming Data Pipeline Architecture](images/architecture.png)

---

## High-Level Data Flow

```text
Uber-Style Web Application
        ↓
Kafka-Compatible Producer
        ↓
Azure Event Hubs
        ↓
Azure Databricks
        ↓
Spark Structured Streaming / Spark Declarative Pipelines
        ↓
Delta Lake
        ↓
Bronze → Silver → Gold
        ↓
Star Schema / Analytics Warehouse
        ↓
Power BI / Analytics Dashboards
```

Batch ingestion flow:

```text
Azure Blob Storage / ADLS JSON Files
        ↓
Azure Data Factory
        ↓
Databricks Bronze Tables
        ↓
Silver and Gold Transformations
```

CI/CD flow:

```text
Developer Code Changes
        ↓
GitHub Repository
        ↓
GitHub Actions CI/CD
        ↓
Code Validation / Tests / Build Checks
        ↓
Databricks Workflow and Pipeline Deployment Readiness
```

---

## Tech Stack

| Category | Tools / Services |
|---|---|
| Cloud Platform | Microsoft Azure |
| Streaming Ingestion | Azure Event Hubs |
| Messaging Compatibility | Kafka-compatible API |
| Batch Orchestration | Azure Data Factory |
| Data Processing | Azure Databricks, Apache Spark |
| Streaming Processing | Spark Structured Streaming |
| Pipeline Framework | Spark Declarative Pipelines / Lakeflow |
| Storage Format | Delta Lake |
| Data Modeling | Star Schema, Fact Tables, Dimension Tables |
| Programming | Python, PySpark, SQL |
| CI/CD | GitHub Actions |
| Version Control | GitHub |
| Reporting Layer | Power BI / Analytics Dashboards |

---

## Key Features

- Simulated Uber-style ride booking data generation
- Real-time event streaming using Azure Event Hubs
- Kafka-compatible producer integration
- Batch ingestion of historical and reference JSON files using Azure Data Factory
- Databricks-based batch and streaming processing
- Spark Declarative Pipelines / Lakeflow implementation
- Bronze, Silver, and Gold Delta Lake architecture
- Historical bulk load combined with live streaming ride events
- JSON parsing, schema enforcement, and type standardization
- Data cleansing, deduplication, and validation
- Star schema design for analytical reporting
- Fact and dimension table modeling
- CI/CD workflow using GitHub Actions
- Deployment-ready project structure

---

## Data Sources

### 1. Streaming Ride Events

Live ride booking events are generated from a simulated Uber-style application and sent to Azure Event Hubs through a Kafka-compatible producer.

ride event fields:

```text
ride_id
confirmation_number
passenger_id
driver_id
vehicle_id
pickup_location_id
dropoff_location_id
payment_method_id
ride_status_id
booking_timestamp
pickup_timestamp
dropoff_timestamp
distance_miles
duration_minutes
base_fare
distance_fare
time_fare
surge_multiplier
tip_amount
total_fare
rating
```

### 2. Batch and Reference Data

Historical and lookup JSON files are stored in Azure Blob Storage / ADLS and ingested using Azure Data Factory.

Example files:

```text
bulk_rides.json
map_cities.json
map_cancellation_reasons.json
map_payment_methods.json
map_ride_statuses.json
map_vehicle_makes.json
map_vehicle_types.json
```

---

## Medallion Architecture

This project follows the Medallion Architecture pattern: Bronze, Silver, and Gold.

### Bronze Layer

The Bronze layer stores raw or minimally processed data from both batch and streaming sources.

Examples:

```text
uber_project.bronze.bulk_rides
uber_project.bronze.map_cities
uber_project.bronze.map_cancellation_reasons
uber_project.bronze.map_payment_methods
uber_project.bronze.map_ride_statuses
uber_project.bronze.map_vehicle_makes
uber_project.bronze.map_vehicle_types
rides_raw
```

Purpose:

- Store raw historical ride data
- Store raw reference/lookup datasets
- Capture streaming ride events from Azure Event Hubs
- Preserve original data for traceability

### Silver Layer

The Silver layer cleans, standardizes, validates, and combines the data.

Key transformations:

- Parse JSON ride events from Event Hubs
- Convert raw event payloads into structured columns
- Cast timestamp fields into proper timestamp types
- Standardize numeric fields such as fares, ratings, duration, and distance
- Filter invalid ride records
- Remove duplicate ride IDs
- Combine historical bulk ride data with streaming ride events
- Prepare clean data for dimensional modeling



### Gold Layer

The Gold layer contains analytics-ready fact and dimension tables modeled using a star schema.

Gold tables:

```text
fact_rides
dim_passengers
dim_drivers
dim_vehicles
dim_cities
dim_payment_methods
dim_ride_statuses
dim_vehicle_types
dim_vehicle_makes
```

---

## Star Schema Design

The serving layer uses a star schema to support analytical queries and reporting.

### Fact Table

`fact_rides`

Contains ride-level transactional metrics and foreign keys to dimension tables.

Example columns:

```text
ride_id
passenger_id
driver_id
vehicle_id
pickup_city_id
dropoff_city_id
payment_method_id
ride_status_id
vehicle_type_id
vehicle_make_id
distance_miles
duration_minutes
base_fare
distance_fare
time_fare
surge_multiplier
subtotal
tip_amount
total_fare
rating
booking_timestamp
pickup_timestamp
dropoff_timestamp
```

### Dimension Tables

Example dimensions:

```text
dim_passengers
dim_drivers
dim_vehicles
dim_cities
dim_payment_methods
dim_ride_statuses
dim_vehicle_types
dim_vehicle_makes
dim_cancellation_reasons
```

---

## Spark Declarative Pipelines

The project uses Spark Declarative Pipelines / Lakeflow to define streaming and batch transformations in Databricks.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

dp.create_streaming_table(
    name="stg_rides",
    comment="Unified staging table combining historical bulk rides and streaming ride events"
)

@dp.append_flow(
    target="stg_rides",
    name="rides_bulk_initial_load",
    once=True
)
def rides_bulk():
    df = spark.read.table("uber_project.bronze.bulk_rides")

    return (
        df
        .withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
        .withColumn("pickup_timestamp", col("pickup_timestamp").cast("timestamp"))
        .withColumn("dropoff_timestamp", col("dropoff_timestamp").cast("timestamp"))
    )

@dp.append_flow(
    target="stg_rides",
    name="rides_eventhub_stream"
)
def rides_stream():
    df = spark.readStream.table("rides_raw")

    return (
        df
        .withColumn("parsed_rides", from_json(col("rides"), rides_schema))
        .select("parsed_rides.*")
    )
```

---

## Azure Event Hubs Streaming Ingestion

The streaming component uses Azure Event Hubs with Kafka-compatible configuration. The simulated application sends ride booking events to Event Hubs. Databricks reads these events as a streaming source and stores them as raw event records in `rides_raw`.

---

## Azure Data Factory Batch Ingestion

Azure Data Factory is used to ingest batch JSON files from Azure Blob Storage / ADLS, including historical ride data and reference mapping files. ADF helps orchestrate file movement and initial ingestion before the data is processed in Databricks.

---

## CI/CD Implementation

This project includes a CI/CD approach using GitHub Actions.

### CI/CD Responsibilities

- Validate Python syntax
- Validate PySpark scripts
- Validate SQL scripts
- Validate JSON configuration files
- Run unit tests
- Check repository structure
- Prepare Databricks pipeline code for deployment
- Support deployment readiness for workflows and configuration files

### CI/CD Flow

```text
Developer pushes code
        ↓
GitHub Actions workflow starts
        ↓
Install dependencies
        ↓
Run lint checks
        ↓
Run unit tests
        ↓
Validate JSON / SQL / PySpark files
        ↓
Prepare deployment-ready artifacts
```

---

## Repository Structure

```text
Real-Time-Uber-Ride-Streaming-Pipeline/
│
├── app/
│   ├── producer.py
│   ├── data_generator.py
│   └── requirements.txt
│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   ├── linked_services/
│   └── arm_templates/
│
├── databricks/
│   ├── ingest.py
│   ├── bronze_adls.py
│   ├── silver.py
│   ├── gold.py
│   └── scd_dimensions.py
│
├── sql/
│   ├── create_star_schema.sql
│   └── validation_queries.sql
│
├── tests/
│   ├── test_data_generator.py
│   └── test_schema_validation.py
│
├── images/
│   └── architecture.png
│
├── .github/
│   └── workflows/
│       └── ci-cd.yml
│
└── README.md
```

---


## Project Highlights

- Built a real-time streaming pipeline using Azure Event Hubs and Kafka-compatible ingestion
- Used Azure Data Factory for batch ingestion of historical and reference JSON files
- Processed streaming and batch data in Azure Databricks
- Implemented Spark Declarative Pipelines / Lakeflow for structured data processing
- Designed Bronze, Silver, and Gold Delta Lake layers
- Combined historical bulk data with real-time streaming events
- Applied schema parsing, type casting, deduplication, and validation
- Created a star schema with fact and dimension tables
- Added GitHub Actions CI/CD for validation and deployment readiness


## Skills Demonstrated

- Real-time data streaming
- Kafka-compatible event ingestion
- Azure Event Hubs integration
- Azure Data Factory orchestration
- Databricks data engineering
- Spark Structured Streaming
- Spark Declarative Pipelines / Lakeflow
- Delta Lake architecture
- Bronze, Silver, and Gold data modeling
- Star schema design
- Fact and dimension modeling
- CI/CD with GitHub Actions
- PySpark and SQL development
- Data validation and transformation

---

## Author

**Jeshwanth Premkumar**  
Data Engineering | Azure | Databricks | Spark | Delta Lake | Streaming Pipelines
