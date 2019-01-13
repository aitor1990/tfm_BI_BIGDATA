#!/bin/bash

airflow connections -a --conn_id spark --conn_type http --conn_host spark --conn_port 8998 &
airflow webserver -p 8081 &
airflow scheduler
