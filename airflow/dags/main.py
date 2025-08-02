from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id= "main",
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 10, 30),
        "email": ["airflow@airflow.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1, #so lan thu lai khi task false
        "retry_delay": timedelta(minutes = 1) #thoi gian cho giua cac lan retry
        },
    schedule_interval=timedelta(days=1), #tan suat kich hoat dag
    catchup=False, #chay lai tat ca DAG bi bo lo tu start_date
) as dag:
    extract_load_task = BashOperator(
    task_id = "extract_load_task",
    bash_command = "spark-submit /opt/airflow/code/extract.py", 
    execution_timeout=timedelta(minutes=8)
    )
    
    transform_google_play_task = SparkSubmitOperator(
        task_id = "transform_google_play_task",
        conn_id = "spark-connection",
        # jars="/opt/airflow/code/postgresql-42.2.5.jar",
        packages="org.postgresql:postgresql:42.2.5",
        application = "/opt/airflow/code/transform.py",
        dag = dag
    )
    extract_load_task  >> transform_google_play_task 
