
from taskmanager import throw_task, validate_task
from airflow import DAG
from datetime import datetime, timedelta

COUNTRY_DIMENSION_ETL_PATH = '/root/airflow/dags/etl/country_dimension.scala'
TOURISM_FACT_ETL_PATH = '/root/airflow/dags/etl/tourism_facts.scala'
LABOUR_FACT_ETL_PATH = '/root/airflow/dags/etl/labour_facts.scala'

TOURISM_DATA_PATH = '/data/raw/urb_ctour.tsv'
LABOUR_DATA_PATH = '/data/raw/urb_clma.tsv'
COUNTRIES_DATA_PATH = '/data/raw/countries.csv'

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
# init dag
dag = DAG('etl-spark', default_args=default_args, schedule_interval=None)

# files to process validators
validator_conf = [
    {'name': 'toursim_data_validator', 'path': TOURISM_DATA_PATH},
    {'name': 'labour_data_validator', 'path': LABOUR_DATA_PATH},
    {'name': 'countries_validator', 'path': COUNTRIES_DATA_PATH}
]
join = validate_task(validator_conf, dag)

# etl tasks
country_etl_task = throw_task(dag, join, COUNTRY_DIMENSION_ETL_PATH, 'country_dimension', debug=False)
throw_task(dag, country_etl_task, TOURISM_FACT_ETL_PATH, 'tourism_facts', debug=True)
throw_task(dag, country_etl_task, LABOUR_FACT_ETL_PATH, 'labour_facts', debug=True)
