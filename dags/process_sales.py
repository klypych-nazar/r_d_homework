import os
from datetime import datetime

import httpx
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from httpx import Response
from airflow.hooks.base import BaseHook


BASE_DIR = Variable.get('BASE_DIR')


def get_server_url(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    return f"{conn.schema}://{conn.host}:{conn.port}"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
}


def trigger_task_1(date: str) -> bool:
    raw_dir: str = os.path.join(BASE_DIR, 'raw', 'sales', date)
    resp: Response = httpx.post(
        url=get_server_url('extract_from_api'),
        json={
            "date": date,
            "raw_dir": raw_dir
        }
    )
    if resp.status_code == 201:
        return True
    return False


def trigger_task_2(date: str) -> bool:
    raw_dir: str = os.path.join(BASE_DIR, 'raw', 'sales', date)
    stg_dir: str = os.path.join(BASE_DIR, 'stg', 'sales', date)
    resp: Response = httpx.post(
        url=get_server_url('json_to_avro'),
        json={
            "raw_dir": raw_dir,
            "stg_dir": stg_dir
        }
    )
    if resp.status_code == 201:
        return True
    return False


with DAG(
        dag_id='process_sales',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 12),
        schedule_interval='0 1 * * *',
        catchup=True,
        default_args=default_args
) as dag:

    task_1 = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=trigger_task_1,
        op_kwargs={'date': '{{ ds }}'}
    )

    task_2 = PythonOperator(
        task_id='convert_to_avro',
        python_callable=trigger_task_2,
        op_kwargs={'date': '{{ ds }}'}
    )

    task_1 >> task_2
