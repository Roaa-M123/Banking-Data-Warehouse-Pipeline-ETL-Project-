import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType


# Configuration

os.environ["HADOOP_USER_NAME"] = "root"

HDFS_BRONZE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/bank_transactions/"
HDFS_SILVER = "hdfs://hadoop-namenode:9000/user/root/datalake/silver/bank_transactions/"
GOLD_BASE   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"


# spark session creation

spark = SparkSession.builder \
    .appName("BankETL_Transformation") \
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

# function to write dim tables
def write_dim_incremental(df, path, key_col, name):
    print(f"\n Writing {name}...")

    try:
        df_clean = df.dropDuplicates([key_col])

        existing = spark.read.parquet(path).select(key_col).distinct()

        new_only = df_clean.join(
            existing,
            on=key_col,
            how="left_anti"
        )

        count_new = new_only.count()

        if count_new > 0:
            print(f" Adding {count_new} new records to {name}")
            new_only.write.mode("append").parquet(path)
        else:
            print(f" No new records for {name}")

    except Exception:
        print(f" First run → writing full {name}")
        df.dropDuplicates([key_col]).write.mode("overwrite").parquet(path)

# dim_date
def generate_static_date_dim(spark, start_date="2019-01-01", end_date="2030-12-31"):
    df = spark.sql(f"SELECT CAST('{start_date}' AS DATE) as start, CAST('{end_date}' AS DATE) as end")    
    df = df.select(
        F.explode(
            F.sequence(F.to_date("start"), F.to_date("end"), F.expr("interval 1 day"))
        ).alias("date")
    )

    dim_date_IF = df.select(
         F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
         "date",
         F.year("date").alias("year"),
         F.month("date").alias("month"),
         F.dayofmonth("date").alias("day"),
         F.date_format("date", "EEEE").alias("day_name"),
         F.dayofweek("date").alias("day_of_week"),
         F.weekofyear("date").alias("week_of_year"),
         F.quarter("date").alias("quarter"),
         F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"))
    
    return dim_date_IF

try:
   
    # READ BRONZE layer
   
    print("reading from bronze layer...")
    df = spark.read.parquet(HDFS_BRONZE)
    print(f"Records loaded: {df.count():,}")

    
    # drop unimportant columns
    
    print("Dropping unimportant columns...")
    cols_to_drop = ["DeviceID","IP Address","source", "ingested_at", "batch_id"]
    df = df.drop(*cols_to_drop)
    print(f"Dropped: {cols_to_drop}")
    print(f"Remaining cols: {df.columns}")

    
    # removeDuplicatesRow
    
    print("\n removing duplicates...")
    before = df.count()
    df = df.dropDuplicates(["TransactionID"])
    after = df.count()
    print(f"Removed {before - after:,} duplicate rows ({before:,} → {after:,})")

    
    # handle null values
    
    print("\n handling null values...")
    df = df.fillna({
        "TransactionAmount":   0.0,
        "TransactionDuration": 0,
        "AccountBalance":      0.0,
        "CustomerAge":         0,
        "LoginAttempts":       0,
    })
    df = df.fillna({
        "TransactionType":    "Unknown",
        "Channel":            "Unknown",
        "Location":           "Unknown",
        "MerchantID":         "Unknown",
        "CustomerOccupation": "Unknown",
    })
    print("null values handled (numeric → 0, categorical → 'Unknown')")

   
    #  dataTypeCasting
   
    print("\n fixing data types...")
    df = df \
        .withColumn("TransactionDate", F.to_timestamp(df["TransactionDate"], "M/d/yyyy HH:mm"))\
        .withColumn("TransactionAmount",df["TransactionAmount"].cast(DoubleType())) \
        .withColumn("TransactionDuration", df["TransactionDuration"].cast(IntegerType())) \
        .withColumn("AccountBalance",df["AccountBalance"].cast(DoubleType())) \
        .withColumn("CustomerAge",df["CustomerAge"].cast(IntegerType())) \
        .withColumn("LoginAttempts",df["LoginAttempts"].cast(IntegerType()))
    print("types fixed")

    #  Save Silver layer 
    df.write.mode("append").format("parquet").save(HDFS_SILVER)
    print(f"Silver layer written to: {HDFS_SILVER}")
    

    
    # starschema ->gold layer
    
    print(" building Star Schema ...")

    dim_date=generate_static_date_dim(spark)
    # dim_customer
    dim_customer = df.select("AccountID", "CustomerAge", "CustomerOccupation") \
        .distinct() \
        .withColumn("customer_key", F.md5(F.col("AccountID"))) \
        .select("customer_key", "AccountID", "CustomerAge", "CustomerOccupation")


    #dim_location 
    dim_location = df.select("Location").distinct() \
        .withColumn("location_key", F.md5(F.col("Location"))) \
        .select("location_key", "Location")

    # dim_transaction_type
    dim_transaction_type = df.select("TransactionType").distinct() \
        .withColumn("transaction_type_key",  F.md5(F.col("TransactionType"))) \
        .select("transaction_type_key", "TransactionType")

    # dim_junk (Channel + MerchantID) 
    dim_junk = df.select("Channel","MerchantID").distinct() \
        .withColumn("junk_key", F.md5(
            F.concat_ws("|", F.col("Channel"), F.col("MerchantID"))
        )) \
        .select("junk_key", "Channel","MerchantID")
    

    # fact_transactions => join عشان محسبهاش تاني هنا(one source for truth)
    fact_transactions = df \
    .withColumn("date_key", F.date_format(F.col("TransactionDate"), "yyyyMMdd").cast("int"))\
    .join(dim_customer,         on="AccountID",       how="left") \
    .join(dim_location,         on="Location",        how="left") \
    .join(dim_transaction_type, on="TransactionType", how="left") \
    .join(
        dim_junk,
        on=(
            (df["Channel"]    == dim_junk["Channel"]) &
            (df["MerchantID"] == dim_junk["MerchantID"])
        ),
        how="left"
    ) \
    .select(
        "TransactionID",
        "customer_key",
        "transaction_type_key",
        "date_key",
        "location_key",
        "junk_key",
        "TransactionAmount",
        "TransactionDuration",
        "AccountBalance",
        "LoginAttempts",
    )

    #  Write Dims

    print("Writing dim_date...")

    dim_date_path = f"{GOLD_BASE}dim_date"


    try:
       existing_df = spark.read.parquet(dim_date_path)
    
       if existing_df.count() > 0:
          print("dim_date already populated — skipping")
       else:
          print("dim_date exists but EMPTY — regenerating...")
          dim_date.write.mode("overwrite").parquet(dim_date_path)

    except Exception:
        print("dim_date not found — generating...")
        dim_date.write.mode("overwrite").parquet(dim_date_path)
    

    
    print("Writing dim_customer...")
    
    write_dim_incremental(
    dim_customer,
    f"{GOLD_BASE}dim_customer",
    "customer_key",
    "dim_customer")


    print("Writing dim_location...")
    write_dim_incremental(
    dim_location,
    f"{GOLD_BASE}dim_location",
    "location_key",
    "dim_location")

 
    
    print("Writing dim_transaction_type...")
    write_dim_incremental(
    dim_transaction_type,
    f"{GOLD_BASE}dim_transaction_type",
    "transaction_type_key",
    "dim_transaction_type")
    
    print("Writing dim_junk...")
    write_dim_incremental(
    dim_junk,
    f"{GOLD_BASE}dim_junk",
    "junk_key",
    "dim_junk")

    print("Writing fact_transactions...")
    fact_transactions = fact_transactions.dropDuplicates(["TransactionID"])
    fact_transactions.write.mode("overwrite").parquet(f"{GOLD_BASE}fact_transactions")
    # fact_transactions.write.mode("append").parquet(f"{GOLD_BASE}fact_transactions")
    print("  fact_transactions ")


    print("\n Gold Star Schema written successfully.")
    

except Exception as e:
    print(f"\n  Transformation failed: {e}")
    raise

finally:
    spark.stop()
    print(" Spark session stopped.")