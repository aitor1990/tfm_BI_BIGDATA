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
                 http_conn_id='http_default',
                 *args, **kwargs):
        super(SparkLivykHook, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.extra_options = extra_options or {}
        self.type = type

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        return str(json.loads(response.text)['id'])
