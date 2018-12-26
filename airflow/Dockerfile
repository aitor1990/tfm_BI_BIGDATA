FROM python:3.6-slim

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

#install airflow
RUN apt-get update
RUN apt-get -y install gcc
RUN pip install requests
RUN pip install --trusted-host pypi.python.org apache-airflow

#get custom airflow configuration
COPY airflow.cfg /root/airflow/airflow.cfg
ADD entrypoint.sh .

RUN chmod +x entrypoint.sh

#aiflow port
EXPOSE 8081

#init airflow state db with sqlite

RUN airflow initdb

#start airflow server and scheduler
ENTRYPOINT ["/entrypoint.sh"]

