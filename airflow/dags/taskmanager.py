from airflow.sensors.http_sensor import HttpSensor
import json
from airflow.operators.http_operator import SimpleHttpOperator
from customtasks import SparkLivykHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator

CONNECTION = 'spark'


def statment_status(response):
    state = response.json()['state']
    if state == 'available':
         return True
    elif state == 'error':
        raise ValueError('error during the process')
    elif state == 'cancelled':
        raise ValueError('task cancelled')
    return False


def validate_task(validators, dag):
    """Validates that all files added in the
    validator exists.

    arguments:
    validators -- list of diactionaries containing both the name of the Sensor
    and the file path
    dag -- Dag where the tasks are added

    returns:
        dummy operator used as join of all sensor tasks"""
    join = DummyOperator(task_id='join_operator', dag=dag)
    for validator in validators:
        sensor = FileSensor(
            task_id=validator['name'], fs_conn_id='fs_default', filepath=validator['path'], dag=dag)
        sensor >> join
    return join


def throw_task(dag, init, code_path, name='', debug=False):
    if name:
        name = '-'+name
    with open(code_path, 'r') as f:
        code = f.read()

    spark_session = SparkLivykHook(
        http_conn_id=CONNECTION,
        task_id='start-session'+name,
        data=json.dumps({'kind': 'spark'}),
        headers={'Content-Type': 'application/json'},
        endpoint='sessions',
        dag=dag,
    )

    sensor = HttpSensor(
        task_id='wait_spark_ready'+name,
        http_conn_id=CONNECTION,
        endpoint="{{'/sessions/'+ti.xcom_pull(task_ids='start-session"
        + name+"')+'/state'}}",
        request_params={},
        response_check=lambda response: response.json()['state'] == 'idle',
        poke_interval=5,
        dag=dag,
    )

    code = SparkLivykHook(
        http_conn_id=CONNECTION,
        task_id='send-task'+name,
        data=json.dumps({'code': code}),
        headers={'Content-Type': 'application/json'},
        endpoint="{{'/sessions/'+ti.xcom_pull(task_ids='start-session"
        + name+"')+'/statements'}}",
        dag=dag,
    )

    end_task = HttpSensor(
        task_id='end-task'+name,
        http_conn_id=CONNECTION,
        endpoint="{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name
        + "')+'/statements/'+ti.xcom_pull(task_ids='send-task"+name+"')}}",
        request_params={},
        response_check=statment_status,
        poke_interval=5,
        dag=dag,
    )

    if not debug:
        close_task = SimpleHttpOperator(
            method='DELETE',
            task_id='close-task'+name,
            http_conn_id=CONNECTION,
            endpoint="{{'/sessions/'+ti.xcom_pull(task_ids='start-session"+name+"')}}",
            dag=dag,
        )

        init >> spark_session >> sensor >> code >> end_task >> close_task
        return close_task
    else:
        init >> spark_session >> sensor >> code >> end_task
        return end_task
