from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils import ingest_weather_data, transform, load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(hours=2),
}

with DAG(
    dag_id='daily_weather_etl',
    default_args=default_args,
    description='Daily ETL pipeline for weather data',
    schedule='0 6 * * *',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=ingest_weather_data,
        do_xcom_push=False
    )

    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform,
        do_xcom_push=True
    )

    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load,
        op_kwargs={
            'data_path': '{{ ti.xcom_pull(task_ids="transform_weather_data") }}'
        }
    )

    extract_task >> transform_task >> load_task
