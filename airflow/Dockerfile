FROM python:3.6

WORKDIR /airflow

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN pip install --trusted-host pypi.python.org apache-airflow

EXPOSE 8080

RUN airflow initdb

CMD airflow webserver -p 8080
