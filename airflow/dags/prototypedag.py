
from airflow import DAG
from lib.taskmanager import throw_task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Aitor',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['aitormarco1990@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl-spark', default_args=default_args, schedule_interval="@once")
throw_task(dag)
