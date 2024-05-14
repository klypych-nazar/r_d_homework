import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


BASE_DIR = Variable.get('BASE_DIR')
BUCKET_NAME = Variable.get('BUCKET_NAME')


def get_server_url(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    return f"{conn.schema}://{conn.host}:{conn.port}"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1
}


with DAG(
        dag_id='upload_to_bucket',
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 11),
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

    upload_file_to_bucket = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_bucket',
        src=os.path.join(BASE_DIR, 'raw', 'sales', '{{ ds }}', f'{"{{ ds }}"}.json'),
        dst=os.path.join('src1', 'sales', 'v1',
                         'year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}',
                         'month={{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}',
                         'day={{ macros.ds_format(ds, "%Y-%m-%d", "%d") }}',
                         '{{ ds }}.json'),
        bucket=BUCKET_NAME,
        mime_type='application/json'
    )

    extract_data_from_api >> upload_file_to_bucket
