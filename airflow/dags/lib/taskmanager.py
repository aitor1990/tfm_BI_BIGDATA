from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import PythonOperator
import json, pprint, requests, textwrap
import logging
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from customtasks import SparkLivykHook
CONNECTION = 'spark'


'''
'''
def throw_task(dag,code_path,name = ''):
    if name:
        name = '-'+name;
    with open(code_path, 'r') as f:
        code = f.read()

    spark_session = SparkLivykHook(
        http_conn_id=CONNECTION,
        task_id='start-session'+name,
        data = json.dumps({'kind': 'spark'}),
        headers = {'Content-Type': 'application/json'},
        endpoint = 'sessions',
        log_response = True,
        dag=dag,
    )


    sensor = HttpSensor(
        task_id='wait_spark_ready'+name,
        http_conn_id=CONNECTION,
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name+"')+'/state'}}",
        request_params={},
        response_check=lambda response : response.json()['state'] == 'idle',
        poke_interval=5,
        dag=dag,
    )

    code = SimpleHttpOperator(
        http_conn_id=CONNECTION,
        task_id='send-task'+name,
        data = json.dumps({'code': code}),
        headers = {'Content-Type': 'application/json'},
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name+"')+'/statements'}}",
        log_response = True,
        dag=dag,
    )

    end_task = HttpSensor(
        task_id='end-task'+name,
        http_conn_id=CONNECTION,
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name+"')+'/state'}}",
        request_params={},
        response_check=lambda response : response.json()['state'] == 'available' or response.json()['state'] == 'idle',
        poke_interval=5,
        dag=dag,
    )

    '''close_task = SimpleHttpOperator(
        method='DELETE',
        task_id='close-task'+name,
        http_conn_id=CONNECTION,
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name+"')}}",
        dag=dag,
    )'''

    spark_session >> sensor >> code >> end_task
    return end_task
