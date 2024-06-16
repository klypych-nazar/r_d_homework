from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, \
    BigQueryInsertJobOperator


PROJECT_NAME = Variable.get('PROJECT_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
        'enrich_user_profiles_pipeline',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['pipeline']
) as dag:
    create_gold_user_profiles_enriched_table = BigQueryCreateEmptyTableOperator(
        task_id='create_gold_user_profiles_enriched_table',
        dataset_id='gold',
        table_id='user_profiles_enriched',
        project_id=PROJECT_NAME,
        schema_fields=[
            {'name': 'client_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'registration_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )

    enrich_user_profiles = BigQueryInsertJobOperator(
        task_id='enrich_user_profiles',
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_NAME}.gold.user_profiles_enriched` T
                    USING (
                        SELECT
                            c.client_id,
                            c.email,
                            COALESCE(c.first_name, u.first_name) AS first_name,
                            COALESCE(c.last_name, u.last_name) AS last_name,
                            COALESCE(c.state, u.state) AS state,
                            SAFE_CAST(c.registration_date AS DATE) AS registration_date,
                            u.birth_date,
                            u.phone_number
                        FROM `{PROJECT_NAME}.silver.customers` c
                        LEFT JOIN `{PROJECT_NAME}.silver.user_profiles` u
                        ON c.email = u.email
                    ) S
                    ON T.client_id = S.client_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            T.first_name = S.first_name,
                            T.last_name = S.last_name,
                            T.state = S.state,
                            T.registration_date = S.registration_date,
                            T.birth_date = S.birth_date,
                            T.phone_number = S.phone_number
                    WHEN NOT MATCHED THEN
                        INSERT (client_id, email, first_name, last_name, state, registration_date, birth_date, phone_number)
                        VALUES (S.client_id, S.email, S.first_name, S.last_name, S.state, S.registration_date, S.birth_date, S.phone_number)
                """,
                "useLegacySql": False,
            }
        }
    )

    create_gold_user_profiles_enriched_table >> enrich_user_profiles
