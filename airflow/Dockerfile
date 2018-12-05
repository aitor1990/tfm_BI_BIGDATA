FROM python:3.6-slim

RUN mkdir /airflow && cd airflow && mkdir /dags

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN apt-get update
RUN apt-get -y install gcc
RUN pip install requests
RUN pip install --trusted-host pypi.python.org apache-airflow
COPY airflow.cfg /root/airflow/airflow.cfg
EXPOSE 8081

RUN airflow initdb

CMD airflow webserver -p 8081
