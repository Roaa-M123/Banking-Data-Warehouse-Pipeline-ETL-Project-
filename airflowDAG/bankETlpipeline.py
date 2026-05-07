from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import os


LANDING_ZONE = "/home/jovyan/work/alterBigDateProj/data_raw_bank_pings/"
SCRIPTS_PATH = "/home/jovyan/work/alterBigDateProj/"

# لازم يبقي زي  start_date  عشان ترتيب ال batches
PIPELINE_START = datetime(2026, 5, 7)

TOTAL_BATCHES = 1000 # number of my batches based on my data

# Airflow يحسب الـ batch_name  من تاريخ الـ run

def resolve_batch(**context):
    try:
        run_date   = context["logical_date"].replace(tzinfo=None)
        run_number = (run_date - PIPELINE_START).days + 1
        
        print(f"  run_date   : {run_date}")
        print(f"  run_number : {run_number}")
        print(f"  PIPELINE_START : {PIPELINE_START}")

        if run_number > TOTAL_BATCHES or run_number < 1:
            print(f"  run_number={run_number} out of range.")
            return False

        batch_file = os.path.join(LANDING_ZONE, f"bank_batch_{run_number:04d}.json")
        context["ti"].xcom_push(key="batch_file", value=batch_file)
        context["ti"].xcom_push(key="run_number", value=run_number)

        print(f"   batch_file: {batch_file}")
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False


# default_args = {
#     "owner":            "airflow",
#     "retries":          2,
#     "retry_delay":      timedelta(minutes=5),
#     "email_on_failure": False,
#     "email_on_retry":   False,
# }

# DAG

with DAG(
    dag_id            = "bank_etl_pipeline",
    # default_args      = default_args,
    description       = "Daily bank ETL — one new batch per run via batch_name",
    start_date     = datetime(2026, 5, 7),
    schedule_interval = "@daily",
    catchup           = False,
) as dag:

    # task 1--> Airflow بيجيب الـ batch_name 
    resolve_batch_task = ShortCircuitOperator(
        task_id         = "resolve_batch",
        python_callable = resolve_batch,
        provide_context = True,
    )

    # task2--> extraction
    extract = BashOperator(
        task_id      = "extract_to_bronze",
        bash_command = (
            "docker exec spark-jupyter spark-submit "
            f"{SCRIPTS_PATH}extraction.py "
            "\"{{ ti.xcom_pull(task_ids='resolve_batch', key='batch_file') }}\""
        )
    )

    #  task3 -->Transform  
    transform = BashOperator(
        task_id      = "transform_to_gold",
        bash_command = (
            f"docker exec spark-jupyter spark-submit {SCRIPTS_PATH}transformation.py"
        ),
    )

    # task4--> Archive Bronze Parquet(الداتا بتاعت الbatch بعد ما حصلها transformation + star schema ==> بوديها ال archive عشان امنع انه يشتغل عليها تاني عشان ميحصلش duplicates)
    archive_bronze = BashOperator(
        task_id      = "archive_bronze_hdfs",
        bash_command = (
            "docker exec hadoop-namenode bash -c \""
            "hdfs dfs -mkdir -p /user/root/datalake/bronze/archived/ && "
            "hdfs dfs -mv /user/root/datalake/bronze/bank_transactions/*.parquet "
            "/user/root/datalake/bronze/archived/ || true\""
        ),
    )

    # task5--> load to snowflake
    load = BashOperator(
        task_id      = "load_to_snowflake",
        bash_command = (
            "docker exec spark-jupyter spark-submit "
            "--packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,"
            "net.snowflake:snowflake-jdbc:3.15.0 "
            f"{SCRIPTS_PATH}loading.py"
        ),
        # execution_timeout = timedelta(minutes=30), 
    )


    resolve_batch_task >> extract >> transform >> archive_bronze >> load