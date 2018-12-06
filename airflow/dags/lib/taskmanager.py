from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import PythonOperator
import json, pprint, requests, textwrap
import logging
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SparkLivykHook(BaseOperator):
    template_fields = ('endpoint', 'data',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 xcom_push=False,
                 http_conn_id='http_default',
                 log_response=False,
                 *args, **kwargs):
        super(SparkLivykHook, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push
        self.log_response = log_response

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

        return str(json.loads(response.text)['id'])

def throw_task(dag):
    CONNECTION = 'spark'
    spark_session = SparkLivykHook(
        http_conn_id=CONNECTION,
        task_id='start-session',
        data = json.dumps({'kind': 'spark'}),
        headers = {'Content-Type': 'application/json'},
        endpoint = 'sessions',
        log_response = True,
        dag=dag,
    )


    sensor = HttpSensor(
        task_id='wait_spark_ready',
        http_conn_id=CONNECTION,
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session')+'/state'}}",
        request_params={},
        response_check=lambda response : response.json()['state'] == 'idle',
        poke_interval=5,
        dag=dag,
    )

    code = SimpleHttpOperator(
        http_conn_id=CONNECTION,
        task_id='send-task',
        data = json.dumps({'code': '1 + 1'}),
        headers = {'Content-Type': 'application/json'},
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session')+'/statements'}}",
        log_response = True,
        dag=dag,
    )

    end_task = HttpSensor(
        task_id='end-task',
        http_conn_id=CONNECTION,
        endpoint = "{{'/sessions/'+ti.xcom_pull(task_ids='start-session')+'/state'}}",
        request_params={},
        response_check=lambda response : response.json()['state'] == 'available' or response.json()['state'] == 'idle',
        poke_interval=5,
        dag=dag,
    )

    spark_session >> sensor >> code >> end_task
    return end_task
