from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import pendulum

default_args = {
    'owner': 'test',
    'retries': 0,
}

with DAG(
    dag_id='test_simple',
    default_args=default_args,
    description='Simple test DAG',
    start_date=pendulum.datetime(2025, 10, 27, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['test']
) as dag:

    def hello():
        print("Hello from test DAG!")
        return "success"

    t1 = PythonOperator(
        task_id='test_task',
        python_callable=hello,
    )
