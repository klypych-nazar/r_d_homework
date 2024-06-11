from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

BUCKET_NAME = Variable.get('BUCKET_NAME')
PROJECT_NAME = Variable.get('PROJECT_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


with DAG(
        'process_user_profiles_pipeline',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['pipeline']
) as dag:
    create_user_profiles_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_user_profiles_external_table',
        table_resource={
            "tableReference": {
                "projectId": PROJECT_NAME,
                "datasetId": "bronze",
                "tableId": "external_user_profiles"
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "sourceUris": [f"gs://{BUCKET_NAME}/user_profiles/user_profiles.json"],
                "schema": {
                    "fields": [
                        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"}
                    ]
                }
            }
        }
    )

    create_empty_silver_table = BigQueryCreateEmptyTableOperator(
        task_id='create_empty_silver_table',
        project_id=PROJECT_NAME,
        dataset_id='silver',
        table_id='user_profiles',
        schema_fields=[
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        if_exists='ignore'
    )

    clean_and_load_to_silver = BigQueryInsertJobOperator(
        task_id='clean_and_load_to_silver',
        configuration={
            "query": {
                "query": f"""
                        INSERT INTO `{PROJECT_NAME}.silver.user_profiles` (email, first_name, last_name, state, birth_date, phone_number)
                        SELECT
                            email,
                            SPLIT(full_name, ' ')[OFFSET(0)] AS first_name,
                            SPLIT(full_name, ' ')[OFFSET(1)] AS last_name,
                            state,
                            SAFE_CAST(birth_date AS DATE) AS birth_date,
                            CONCAT(
                                '+1-',
                                SUBSTR(REGEXP_REPLACE(phone_number, r'\D', ''), 1, 3), '-',
                                SUBSTR(REGEXP_REPLACE(phone_number, r'\D', ''), 4, 3), '-',
                                SUBSTR(REGEXP_REPLACE(phone_number, r'\D', ''), 7, 4)
                            ) AS phone_number
                        FROM `{PROJECT_NAME}.bronze.external_user_profiles`
                        """,
                "useLegacySql": False,
            }
        }
    )

    create_user_profiles_external_table >> create_empty_silver_table >> clean_and_load_to_silver
