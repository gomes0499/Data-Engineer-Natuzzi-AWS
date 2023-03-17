from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "my_pipeline_dag",
    default_args=default_args,
    description="A simple pipeline DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

api_task = BashOperator(
    task_id="api",
    bash_command="python3 /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/scripts/api.py",
    dag=dag,
)

data_ingestion_task = BashOperator(
    task_id="data_ingestion",
    bash_command="python3 /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/scripts/data-ingestion.py",
    dag=dag,
)

gluejob_task = BashOperator(
    task_id="gluejob",
    bash_command="python3 /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/scripts/gluejob.py",
    dag=dag,
)

redshift_task = BashOperator(
    task_id="redshift",
    bash_command="python3 /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/scripts/redshift.py",
    dag=dag,
)

dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='cd /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/wu1project && dbt run',
    dag=dag,
)

redshift_to_s3_task = BashOperator(
    task_id="redshift_to_s3",
    bash_command="python3 /Users/gomes/Desktop/Projects/Data\ Engineer/1-Project/scripts/redshift_to_s3.py",
    dag=dag,
)

api_task >> data_ingestion_task >> gluejob_task  >> redshift_task >> dbt_run_task >> redshift_to_s3_task
