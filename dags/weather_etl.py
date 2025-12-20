from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.timezone import datetime

from utils import ingest_weather_data, transform, load


def debug_env():
    import os 
    print(f"Postgres string: {os.getenv("POSTGRES_STRING")}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1,),
}

with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    description='Hourly ETL pipeline for weather data',
    schedule='@hourly',
    catchup=False,
    tags=['weather', 'etl'],
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
    
    debug_task = PythonOperator(
        task_id='debug_env',
        python_callable=debug_env,
    )

    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load,
        op_kwargs={
            'data_path': '{{ ti.xcom_pull(task_ids="transform_weather_data") }}'
        }
    )

    extract_task >> transform_task >> debug_task >> load_task
