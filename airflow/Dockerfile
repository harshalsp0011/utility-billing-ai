FROM apache/airflow:2.10.2
USER root
RUN pip install psycopg2-binary pandas
USER airflow
COPY dags/ /opt/airflow/dags/
