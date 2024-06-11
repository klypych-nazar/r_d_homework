from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
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
        tags=['pipeline']
) as dag:
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

    create_empty_silver_table = BigQueryCreateEmptyTableOperator(
        task_id='create_empty_silver_table',
        project_id=PROJECT_NAME,
        dataset_id='silver',
        table_id='sales',
        schema_fields=[
            {'name': 'client_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'purchase_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'product_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'price', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ],
        time_partitioning={
            "type": "DAY",
            "field": "purchase_date"
        },
        if_exists='ignore'
    )

    clean_and_load_to_silver = BigQueryInsertJobOperator(
        task_id='clean_and_load_to_silver',
        configuration={
            "query": {
                "query": f"""
                        INSERT INTO `{PROJECT_NAME}.silver.sales` (client_id, purchase_date, product_name, price)
                        SELECT
                            SAFE_CAST(CustomerId AS INTEGER) AS client_id,
                            SAFE_CAST(PurchaseDate AS DATE) AS purchase_date,
                            UPPER(Product) AS product_name,
                            SAFE_CAST(REPLACE(Price, '$', '') AS INTEGER) AS price
                        FROM `{PROJECT_NAME}.bronze.external_sales`
                    """,
                "useLegacySql": False,
            }
        }
    )

    create_sales_external_table >> create_empty_silver_table >> clean_and_load_to_silver
