from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from google.cloud import storage

BUCKET_NAME = Variable.get('BUCKET_NAME')
PROJECT_NAME = Variable.get('PROJECT_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


def list_csv_files(prefix='customers/'):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    return [file for file in files if file.split('/')[2].startswith(file.split('/')[1])]


with DAG(
    'process_customers_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pipeline']
) as dag:

    create_customers_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_customers_external_table',
        destination_project_dataset_table=f'{PROJECT_NAME}.bronze.external_customers',
        bucket=BUCKET_NAME,
        source_objects=list_csv_files(),
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        dag=dag
    )

    create_empty_silver_table = BigQueryCreateEmptyTableOperator(
        task_id='create_empty_silver_table',
        project_id=PROJECT_NAME,
        dataset_id='silver',
        table_id='customers',
        schema_fields=[
            {'name': 'client_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'registration_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        if_exists='ignore'
    )

    clean_and_load_to_silver = BigQueryInsertJobOperator(
        task_id='clean_and_load_to_silver',
        configuration={
            "query": {
                "query": f"""
                        INSERT INTO `{PROJECT_NAME}.silver.customers` (client_id, first_name, last_name, email, registration_date, state)
                        SELECT 
                            SAFE_CAST(Id AS INTEGER) AS client_id, 
                            FirstName AS first_name, 
                            LastName AS last_name, 
                            Email AS email, 
                            SAFE_CAST(RegistrationDate AS DATE) AS registration_date, 
                            State AS state
                        FROM `{PROJECT_NAME}.bronze.external_customers`
                        """,
                "useLegacySql": False,
            }
        }
    )

    create_customers_external_table >> create_empty_silver_table >> clean_and_load_to_silver
