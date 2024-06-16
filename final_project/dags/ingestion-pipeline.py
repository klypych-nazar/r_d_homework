import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

BUCKET_NAME = Variable.get('BUCKET_NAME')
SRC_PATH = Variable.get('SRC_PATH')
FOLDERS = Variable.get("FOLDERS", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


def create_upload_tasks(folder, local_base_path, bucket_name, dag):
    tasks = []
    for root, _, files in os.walk(os.path.join(local_base_path, folder)):
        for file in files:
            local_file_path = os.path.join(root, file)
            remote_path = os.path.relpath(path=local_file_path, start=local_base_path)
            task = LocalFilesystemToGCSOperator(
                task_id=f'upload_{remote_path.replace("/", "_")}',
                src=local_file_path,
                dst=remote_path,
                bucket=bucket_name,
                dag=dag
            )
            tasks.append(task)
    return tasks


with DAG(
    dag_id='ingestion-pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pipeline', 'ingestion']
) as dag:

    upload_tasks = []

    for folder in FOLDERS:
        upload_tasks.extend(
            create_upload_tasks(
                folder=folder,
                local_base_path=SRC_PATH,
                bucket_name=BUCKET_NAME,
                dag=dag
            )
        )
