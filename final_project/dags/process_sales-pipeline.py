from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage


BUCKET_NAME = Variable.get('BUCKET_NAME')
PROJECT_NAME = Variable.get('PROJECT_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


def list_csv_files(prefix='sales/'):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    return files


with DAG(
    'process_sales_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pipeline', 'bronze']
) as dag:

    # Step 1: Load data into Bronze
    create_sales_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_sales_external_table',
        destination_project_dataset_table=f'{PROJECT_NAME}.bronze.external_sales',
        bucket=BUCKET_NAME,
        source_objects=list_csv_files(),
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1
    )

    create_sales_external_table
