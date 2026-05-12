# Banking-Data-Warehouse-Pipeline-ETL-Project (bankingTransactionAnalysis)

## Overview

This project is an end-to-end data engineering pipeline that processes raw banking transactions into a structured data warehouse using a **Bronze → Silver → Gold architecture**. It ensures data quality, consistency, and analytics readiness.

The pipeline uses **Apache Spark** for distributed processing, **Apache Airflow** for orchestration, and **Snowflake + HDFS** for storage and analytics.

---

## Architecture

This project follows a **modern Data Lakehouse architecture** based on a multi-layer ETL pipeline:

### Bronze Layer (Raw Data)
- Raw banking transaction data is ingested from CSV files
- Data is split into 50-row batches using a simulator
- Each batch is stored in a landing zone as a JSON file
- Data is then loaded into HDFS in Parquet format (append mode)

---

### Silver Layer (Cleaned Data)
- Data is read from the Bronze layer (HDFS)
- Data cleaning is performed using Apache Spark:
  - Removing duplicates (keyed on TransactionID)
  - Handling null values (numeric → 0, categorical → "Unknown")
  - Fixing data types (timestamps, doubles, integers)
  - Dropping unnecessary columns (DeviceID, IP Address, source, ingested_at, batch_id)
- The cleaned dataset is stored back into HDFS as Parquet files (append mode)

---

### Gold Layer (Data Warehouse — Star Schema)
- Business-ready data is created using a **Star Schema** stored in HDFS Gold
- Dimension tables:

| Table | Load Strategy | Notes |
|---|---|---|
| dim_date | Static | Generated once (2019–2030). Skipped if already populated. |
| dim_customer | Append-only | New AccountIDs appended via left-anti join on MD5 surrogate key. |
| dim_location | SCD Type 0 | Insert-only. Never updated once written. |
| dim_transaction_type | SCD Type 0 | Insert-only. Never updated once written. |
| dim_junk | SCD Type 0 | Channel + MerchantID composite key. Insert-only. |
| fact_transactions | Delta (MERGE) | Matched rows updated; new rows inserted via Snowflake MERGE. |

- Surrogate keys generated using MD5 hashing on natural key column(s)
- Data is optimized for analytics and reporting

---

### Snowflake Layer (Cloud Data Warehouse)
- Gold layer tables are loaded into **Snowflake (BANK_DW.GOLD_LAYER)**
- Supports:
  - **Atomic table swap** for dimension tables (TEMP → FINAL, drop TEMP) — zero downtime
  - **MERGE-based incremental load** for fact table (matched → UPDATE, new → INSERT)
  - `dim_date` is skipped on subsequent runs if already populated

---

## Pipeline Workflow (Airflow Orchestrated)

The entire ETL process is fully automated using an Apache Airflow DAG (`bank_etl_pipeline`) running on a **daily schedule** (`catchup=False`). Each daily run processes exactly one batch.

### 1. Batch Resolution — `resolve_batch` (ShortCircuitOperator)
- Airflow calculates the correct batch number from `logical_date` using `(run_date − start_date).days + 1`
- Validates that run number is within range (1–1000); stops the DAG if out of range
- Pushes `batch_file` path and `run_number` to XCom for downstream tasks

---

### 2. Data Extraction — `extract_to_bronze` (BashOperator)
- Triggers `spark-submit extraction.py` via `docker exec spark-jupyter`
- Reads the selected batch file path from XCom (`sys.argv[1]`)
- Enforces typed `StructType` schema, then appends to HDFS Bronze layer as Parquet

---

### 3. Data Transformation — `transform_to_gold` (BashOperator)
- Triggers `spark-submit transformation.py`
- Cleans Bronze data into Silver (dedup, null fill, type casting, drop cols)
- Builds Star Schema in Gold: all dimension tables + `fact_transactions`

---

### 4. Data Archiving — `archive_bronze_hdfs` (BashOperator)
- Moves processed Bronze Parquet files to `/bronze1/archived/` in HDFS
- Prevents reprocessing and avoids duplicates on the next run

---

### 5. Loading to Snowflake — `load_to_snowflake` (BashOperator)
- Triggers `spark-submit loading.py` with the Snowflake connector package
- Dimension tables loaded via atomic table swap
- `fact_transactions` loaded via MERGE (delta load)
- Staging table (`_STG`) written first, merged into final table, then dropped

---

### Airflow DAG Structure

```
resolve_batch → extract_to_bronze → transform_to_gold → archive_bronze_hdfs → load_to_snowflake
```

---

## Tech Stack

| Tool | Role |
|---|---|
| Apache Spark | Distributed data processing (extraction, transformation) |
| Apache Airflow | Pipeline orchestration & daily scheduling |
| HDFS | Distributed storage for Bronze / Silver / Gold layers |
| Snowflake | Cloud data warehouse — final analytics destination |
| Python | ETL scripting (simulator, Spark jobs, DAG) |
| Docker | Containerised runtime for all services |

---

## Project Structure

```
project/
│
├── docker-compose.yaml
├── README.md
├── simulator.py
│
├── data/
│
├── scripts/
│   ├── extraction.py
│   ├── transformation.py
│   └── loading.py
│
└── airflowDAG/
    └── bank_etl_pipeline.py
```

---

## How to Run the Project

### 1. Prerequisites

Make sure you have the following installed:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

### 2. Start the Environment

Run the following command inside the folder that contains `docker-compose.yaml`:

```bash
docker-compose up -d
```

---

### 3. Open Jupyter Notebook

Navigate to:

```
http://localhost:8888
```

---

### 4. Run the Simulator

Pre-generate all batch files in the landing zone:

```bash
python simulator.py
```

---

### 5. Run Airflow

Place the DAG file (`bank_etl_pipeline.py`) inside the `dags/` directory (in the same folder as `docker-compose.yaml`), then open the Airflow UI:

```
http://localhost:8080
```

- Turn **ON** the DAG: `bank_etl_pipeline`
- Trigger it manually or let it run on its daily schedule

### 6.Architecture Diagram
<img width="1358" height="2158" alt="image" src="https://github.com/user-attachments/assets/4a28cc0b-8ae0-4ac8-ae74-60600e77df61" />


### 7.power pi Dashboard

[![Power BI Dashboard](assets/dashboard_preview.png)](https://app.powerbi.com/groups/71ed6d4b-812b-40b5-81f6-0df68c506901/reports/e56b7285-fe22-4cd3-b3fb-a7a99f3c9ed4/b540fa21b8bfd7375ab3?experience=power-bi)

