
from etl.taskmanager import throw_task
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.sensors.file_sensor import FileSensor

default_args = {
    'owner': 'Aitor',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['aitormarco1990@gmail.com'],
    'email_on_faiure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('etl-spark', default_args=default_args, schedule_interval="@once")
sensor = FileSensor(task_id='wait_csv' ,fs_conn_id= 'fs_default' ,filepath = '/data/urb_ctour.tsv', dag = dag)
end = throw_task(dag,sensor, '/root/airflow/dags/etl/country_dimension.scala', 'country_dimension')
throw_task(dag,end, '/root/airflow/dags/etl/tourism_facts.scala', 'tourism_facts',debug = True)
throw_task(dag,end, '/root/airflow/dags/etl/labour_facts.scala', 'labour_facts',debug = True)
