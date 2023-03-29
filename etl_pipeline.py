from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

with DAG(
    "etl_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="python etl_pipeline DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 3, 29),
    catchup=False,
    tags=["etl_pipeline"],
) as dag:
    t1 = BashOperator(
        task_id="extract",
        bash_command="python /home/ubuntu/airflow/ETL_Pipeline/extract.py"
    )

    t2 = BashOperator(
        task_id="transform",
        depends_on_past=True,
        bash_command="python /home/ubuntu/airflow/ETL_Pipeline/transform.py",
        retries=3
    )

    t3 = BashOperator(
        task_id="load",
        depends_on_past=True,
        bash_command="python /home/ubuntu/airflow/ETL_Pipeline/load.py"
    )

    t1 >> t2 >> t3