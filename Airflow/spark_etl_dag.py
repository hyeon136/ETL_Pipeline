import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from datetime import datetime, timedelta
#-------------Airflow----------------
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
#------------------------------------

def extract():
    """
    1. 지정 url에서 데이터 추출하여 json으로 변환
    2. 데이터를 json 파일로 저장
    """
    import requests
    import json

    url = "http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log"
    data = requests.get(url).json()

    with open("./tmp.json", "w") as f:
        json.dump(data, f)

cmd_transform = "spark-submit ./pyspark_transform.py"
cmd_load = "spark-submit ./pyspark_load.py"

with DAG(
    "test_spark_etl_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    },
    description="python etl_pipeline DAG",
    schedule=timedelta(minutes=3),
    start_date=datetime(2023, 3, 29),
    catchup=False,
    tags=["etl_pipeline"]
) as dag:
    etl_extract = PythonOperator(
        task_id="extract",
        python_callable = extract
    )

    etl_transform = BashOperator(
        task_id="transform",
        bash_command = cmd_transform
    )

    etl_load = BashOperator(
        task_id="load",
        bash_command = cmd_load
    )

    etl_extract >> etl_transform >> etl_load