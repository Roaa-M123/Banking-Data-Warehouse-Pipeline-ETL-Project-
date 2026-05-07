# Banking-Data-Warehouse-Pipeline-ETL-Project(bankingTransactionAnalysis)
##  Overview
This project is an end-to-end data engineering pipeline that processes raw banking transactions into a structured data warehouse using a **Bronze → Silver → Gold architecture**. It ensures data quality, consistency, and analytics readiness.

The pipeline uses **Apache Spark** for distributed processing, **Apache Airflow** for orchestration, and **Snowflake + HDFS** for storage and analytics.

##  Architecture

This project follows a **modern Data Lakehouse architecture** based on a multi-layer ETL pipeline:

###  Bronze Layer (Raw Data)
- Raw banking transaction data is ingested from CSV files
- Data is split into batches using a simulator
- Each batch is stored in a landing zone as JSON files
- Data is then loaded into HDFS in Parquet format

---

###  Silver Layer (Cleaned Data)
- Data is read from the Bronze layer (HDFS)
- Data cleaning is performed using Apache Spark:
  - Removing duplicates
  - Handling null values
  - Fixing data types
  - Dropping unnecessary columns
- The cleaned dataset is stored back into HDFS as Parquet files

---

###  Gold Layer (Data Warehouse)
- Business-ready data is created using a **Star Schema**
- Dimension tables include:
  - Dim Customer
  - Dim Date
  - Dim Location
  - Dim Transaction Type
  - Dim Junk (Channel + Merchant)
- Fact table: `fact_transactions`
- Data is optimized for analytics and reporting

---

### Snowflake Layer (Data Warehouse Storage)
- Gold layer tables are loaded into Snowflake
- Supports:
  - Full load for dimension tables
  - MERGE operations for fact table (incremental updates)
  - Atomic swap strategy for safe loading

---

##  Pipeline Workflow (Airflow Orchestrated)

The entire ETL process is fully automated using an Apache Airflow DAG (`bank_etl_pipeline`) and runs daily.

### 1️⃣ Batch Resolution
- Airflow calculates the correct batch file based on the DAG run date
- Each run processes exactly one batch from the landing zone
- Batch selection is handled dynamically using XCom

---

### 2️⃣ Data Extraction (Bronze Layer)
- Spark job is triggered via Airflow
- Reads the selected batch file from the landing zone
- Loads raw data into HDFS Bronze layer in Parquet format

---

### 3️⃣ Data Transformation (Silver → Gold Layer)
- Spark performs data cleaning and transformation:
  - Removing duplicates
  - Handling null values
  - Type casting
- Builds Star Schema:
  - Dimension tables (Customer, Date, Location, etc.)
  - Fact table (Transactions)

---

### 4️⃣ Data Archiving
- After successful transformation:
  - Processed Bronze Parquet files are moved to archive folder in HDFS
  - Prevents reprocessing and avoids duplicates

---

### 5️⃣ Loading to Snowflake (Gold Layer)
- Final Gold layer tables are loaded into Snowflake
- Supports:
  - Full load for dimension tables
  - MERGE-based incremental load for fact table
- Ensures atomic and consistent warehouse updates

---


### Airflow DAG Structure
The pipeline runs in the following order:

1. `resolve_batch`
2. `extract_to_bronze`
3. `transform_to_gold`
4. `archive_bronze_hdfs`
5. `load_to_snowflake`

---
##  Tech Stack
- Apache Spark
- Apache Airflow
- HDFS
- Snowflake
- Python
- Docker



##  Project Structure
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
│ ├── extraction.py
│ ├── transformation.py
│ ├── loading.py
│
└── airflowDAG/
└── bank_etl_pipeline.py
```
---
##  How to Run the Project

Follow these steps to set up and execute the pipeline:

### 1️⃣ Prerequisites
Make sure you have the following installed:
- Docker
- Docker-compose.yaml

---

### 2️⃣ Start the Environment
Run the following command to start all required services inside folder that contains Docker-compose.yaml:

```bash
docker-compose up -d
```

---

### 3️⃣ Open Jupyter:
[http://localhost:8888](http://localhost:8899/)
---
### 4️⃣ Run Simulator file:

python simulator.py
---
### 5️⃣ Run Airflow

Ensure that the DAG file (`bank_etl_pipeline.py`) is placed inside the `dags/` directory, which must exist in the same project folder as `docker-compose.yaml`.

Then access Airflow UI:

[http://localhost:8080](http://localhost:18080/)

- Turn ON the DAG: `bank_etl_pipeline`
- Trigger it manually or let it run on schedule



