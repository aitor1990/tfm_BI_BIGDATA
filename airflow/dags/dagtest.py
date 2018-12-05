
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import json, pprint, requests, textwrap

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

"""def task_spark()
    r = requests.get(session_url, headers={'Content-Type': 'application/json'})
    statements_url = session_url + '/statements'
    data = {'code': '1 + 1'}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    return r.json()"""

"""def init_session(ds, **kwargs):
    host = 'http://localhost:8998'
    data = {'kind': 'spark'}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    session_url = host + r.headers['location']"""


dag = DAG('etl-spark', default_args=default_args, schedule_interval="@once")
run_this = SimpleHttpOperator(
    http_conn_id='spark',
    task_id='start-session',
    data = json.dumps({'kind': 'spark'}),
    headers = {'Content-Type': 'application/json'},
    endpoint = 'sessions',
    log_response = True,
    xcom_push = True,
    dag=dag,
)
