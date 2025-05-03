from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pytz
import os

# Custom modules
from utils.config import get_spark_jars

# Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, '../', 'scripts')

# Airflow Settings
DAG_ARGUMENTS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'start_date': datetime(2023, 1, 1)
}

SPARK_SETTINGS = {
    'conn_id': "spark_default",
    'verbose': False,
    'env_vars': {
        "PYTHONPATH": SCRIPTS_DIR
    },
    'jars': get_spark_jars()
}

# Task Arguments
ISRAEL_TZ = pytz.timezone("Asia/Jerusalem")
CURRENT_TIME = datetime.now(ISRAEL_TZ)
FETCH_START = CURRENT_TIME - timedelta(hours=4)
FETCH_END = CURRENT_TIME
TASK_ARGUMENTS = [
    "--fetch_start", FETCH_START.strftime("%Y-%m-%dT%H:%M:%S"),
    "--fetch_end", FETCH_END.strftime("%Y-%m-%dT%H:%M:%S")
]

with DAG(
    'fraud_detection',
    schedule_interval=None,
    default_args=DAG_ARGUMENTS,
    catchup=False
) as dag:

    t1 = SparkSubmitOperator(
        **SPARK_SETTINGS,
        task_id="transactions",
        application=f"{SCRIPTS_DIR}/fraud_detection/etl.py",
        application_args=[
            "--method", "load_transactions", 
            *TASK_ARGUMENTS
        ]
    )

    # Set task order
    t1