import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
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


with DAG(
        dag_id='process_sales',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 12),
        schedule_interval='0 1 * * *',
        catchup=True,
        default_args=default_args
) as dag:

    extract_data_from_api = SimpleHttpOperator(
        task_id='extract_data_from_api',
        method='POST',
        data=json.dumps({
            'date': '{{ ds }}',
            'raw_dir': os.path.join(BASE_DIR, 'raw', 'sales', '{{ ds }}')
        }),
        headers={'Content-Type': 'application/json'},
        http_conn_id='extract_from_api',
        response_check=lambda response: response.status_code == 201,
        log_response=True
    )

    convert_to_avro = SimpleHttpOperator(
        task_id='convert_to_avro',
        method='POST',
        data=json.dumps({
            'raw_dir': os.path.join(BASE_DIR, 'raw', 'sales', '{{ ds }}'),
            'stg_dir': os.path.join(BASE_DIR, 'stg', 'sales', '{{ ds }}')
        }),
        headers={'Content-Type': 'application/json'},
        http_conn_id='json_to_avro',
        response_check=lambda response: response.status_code == 201,
        log_response=True
    )

    extract_data_from_api >> convert_to_avro
