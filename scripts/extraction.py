import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

# configuration
os.environ["HADOOP_USER_NAME"] = "root"

HDFS_BRONZE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/bank_transactions/"

# ── Airflow بيبعت اسم الـ file كـ argument ────────────────
if len(sys.argv) < 2:
    print(" No batch file provided. Usage: e.py <batch_file_path>")
    sys.exit(1)

batch_file = sys.argv[1]
batch_name = os.path.basename(batch_file)

print(f"\n  Batch file : {batch_name}")

#expected_schema
schema = StructType([
    StructField("TransactionID",       StringType(),  True),
    StructField("AccountID",           StringType(),  True),
    StructField("TransactionAmount",   DoubleType(),  True),
    StructField("TransactionDate",     StringType(),  True),
    StructField("TransactionType",     StringType(),  True),
    StructField("TransactionDuration", IntegerType(), True),
    StructField("AccountBalance",      DoubleType(),  True),
    StructField("Channel",             StringType(),  True),
    StructField("Location",            StringType(),  True),
    StructField("DeviceID",            StringType(),  True),
    StructField("IP Address",          StringType(),  True),
    StructField("MerchantID",          StringType(),  True),
    StructField("CustomerAge",         IntegerType(), True),
    StructField("CustomerOccupation",  StringType(),  True),
    StructField("LoginAttempts",       IntegerType(), True),
    StructField("source",              StringType(),  True),
    StructField("ingested_at",         StringType(),  True),
    StructField("batch_id",            StringType(),  True),
])

#spark session
spark = SparkSession.builder \
    .appName("BankETL_Extraction") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS","hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname","resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address","resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address","resourcemanager:8030") \
    .config("spark.driver.host","172.30.1.13") \
    .config("spark.driver.bindAddress","0.0.0.0") \
    .config("spark.executor.memory","512m") \
    .config("spark.yarn.am.memory","512m") \
    .getOrCreate()

print("Spark connected successfully")


# read batch after that writting it to HDFS bronze layer 

try:
    raw_df = spark.read \
        .schema(schema) \
        .json(f"file://{batch_file}")

    record_count = raw_df.count()
    print(f"  Records in batch : {record_count}")

    if record_count == 0:
        print(" Batch is empty — skipping write.")
    else:
        print(f"  Writing to HDFS Bronze: {HDFS_BRONZE}")
        raw_df.write \
            .mode("append") \
            .format("parquet") \
            .save(HDFS_BRONZE)
        print(f"batch written to Bronze layer as Parquet.")

except Exception as e:
    print(f"Extraction failed: {e}")
    raise

finally:
    spark.stop()
    print("Spark session stopped.")