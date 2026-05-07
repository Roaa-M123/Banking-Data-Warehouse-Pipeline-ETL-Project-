import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



#configuration

os.environ["HADOOP_USER_NAME"] = "root"

GOLD_BASE = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

#smowflake connection
sf_options = {
    "sfURL":       "ds09979.af-south-1.aws.snowflakecomputing.com",  
    "sfUser":      "roaamansour12",                         
    "sfPassword":  "YmVAYUiB.EFG7!n",                        
    "sfDatabase":  "BANK_DW",
    "sfSchema":    "GOLD_LAYER",
    "sfWarehouse": "BANK_WH",
}

# spark session
spark = SparkSession.builder \
    .appName("BankETL_Loading") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS","hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname","resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address","resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host","172.30.1.13") \
    .config("spark.driver.bindAddress","0.0.0.0") \
    .config("spark.executor.memory","512m") \
    .config("spark.yarn.am.memory","512m") \
    .config("spark.jars.packages","net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.15.0") \
    .getOrCreate()

print("Spark connected successfully")




def load_dim_once_to_snowflake(table_name):

    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")

    try:
        count_query = f"SELECT COUNT(*) AS cnt FROM BANK_DW.GOLD_LAYER.{table_name.upper()}"

        existing_df = spark.read \
            .format("snowflake") \
            .options(**sf_options) \
            .option("query", count_query) \
            .load()

        cnt = existing_df.collect()[0]["CNT"]

        if cnt > 0:
            print(f"{table_name.upper()} already populated — skipping load")
            return

        print(f"{table_name.upper()} exists but EMPTY — loading data...")

    except Exception:
        print(f"{table_name.upper()} not found — creating and loading...")

    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", f"BANK_DW.GOLD_LAYER.{table_name.upper()}") \
        .mode("overwrite") \
        .save()

    print(f"{table_name.upper()} loaded successfully")




def load_to_snowflake(table_name):


    temp_table  = f"BANK_DW.GOLD_LAYER.{table_name.upper()}_TEMP"
    final_table = f"BANK_DW.GOLD_LAYER.{table_name.upper()}"

    print(f"\n  Starting Atomic Load for {final_table}...")

    # Step 1-> اقرأ من HDFS واكتب في TEMP
    print(f"  Reading {table_name} from HDFS...")
    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")
    print(f"  Records: {df.count():,}")

    print(f"  Writing to temp table: {temp_table}...")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()
    try:
        snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

        swap_query = f"ALTER TABLE IF EXISTS {final_table} SWAP WITH {temp_table}"
        snowflake_utils.runQuery(sf_options, swap_query)
        print(f"Swapped {temp_table} → {final_table}")

        drop_query = f"DROP TABLE IF EXISTS {temp_table}"
        snowflake_utils.runQuery(sf_options, drop_query)
        print(f"Temp table dropped")

        print(f"SUCCESS: {final_table} loaded atomically.")

    except Exception as e:
        print(f"Atomic Swap failed for {final_table}: {e}")
        raise e





def load_fact_to_snowflake(table_name):

    target_table = table_name.upper()
    staging_table = f"{target_table}_STG"

    print(f"\n Starting MERGE Load for {target_table}...")

    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")

    # write staging
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", staging_table) \
        .mode("overwrite") \
        .save()

    snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

    # check if table exists
    check_query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{target_table}'
    """

    exists = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", check_query) \
        .load() \
        .collect()[0][0] > 0

    try:
        if not exists:
            print(" First run → creating table with full load")

            # أول مرة → create table
            df.write \
                .format("net.snowflake.spark.snowflake") \
                .options(**sf_options) \
                .option("dbtable", target_table) \
                .mode("overwrite") \
                .save()

        else:
            print(" Table exists → running MERGE")

            merge_query = f"""
            MERGE INTO BANK_DW.GOLD_LAYER.{target_table} t
            USING BANK_DW.GOLD_LAYER.{staging_table} s
            ON t.TransactionID = s.TransactionID

            WHEN MATCHED THEN UPDATE SET
                t.customer_key = s.customer_key,
                t.transaction_type_key = s.transaction_type_key,
                t.date_key = s.date_key,
                t.location_key = s.location_key,
                t.junk_key = s.junk_key,
                t.TransactionAmount = s.TransactionAmount,
                t.TransactionDuration = s.TransactionDuration,
                t.AccountBalance = s.AccountBalance,
                t.LoginAttempts = s.LoginAttempts

            WHEN NOT MATCHED THEN INSERT
            VALUES (
                s.TransactionID,
                s.customer_key,
                s.transaction_type_key,
                s.date_key,
                s.location_key,
                s.junk_key,
                s.TransactionAmount,
                s.TransactionDuration,
                s.AccountBalance,
                s.LoginAttempts
            )
            """

            snowflake_utils.runQuery(sf_options, merge_query)

        # cleanup
        snowflake_utils.runQuery(sf_options, f"DROP TABLE IF EXISTS {staging_table}")

        print(" SUCCESS")

    except Exception as e:
        print(f" Failed: {e}")
        raise e


# run
try:
    print("\n" + "="*55)
    print("  SNOWFLAKE LOAD — Gold Layer → Snowflake")
    print("="*55)

    # Dims — Atomic Swap
    # skip لو موجودة
    load_dim_once_to_snowflake("dim_date")
    load_to_snowflake("dim_customer")
    load_to_snowflake("dim_location")
    load_to_snowflake("dim_transaction_type")
    load_to_snowflake("dim_junk")

    #fact
    load_fact_to_snowflake("fact_transactions")

    print("\n" + "="*55)
    print(" ALL GOLD TABLES LOADED TO SNOWFLAKE")
    print("="*55 + "\n")

except Exception as e:
    print(f"\n Loading failed: {e}")
    raise e

finally:
    spark.stop()
    print("Spark session stopped.")