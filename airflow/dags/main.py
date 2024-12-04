from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    "main",
    default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes = 1)
    },
    schedule_interval=timedelta(days=1), 
    catchup=False,
) as dag:
    extract_load_task = BashOperator(
    task_id = "extract_load_task",
    bash_command = "spark-submit /opt/airflow/code/push_to_hdfs.py", 
     execution_timeout=timedelta(minutes=10)
    )

transform_google_play_task = BashOperator(
    task_id = "transform_google_play_task",
    bash_command = "spark-submit --driver-class-path /opt/airflow/code/postgresql-42.2.5.jar /opt/airflow/code/transform.py", 
    dag = dag
)

extract_load_task >> transform_google_play_task