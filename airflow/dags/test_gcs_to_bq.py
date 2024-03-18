from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import timedelta


# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'catchup' : False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

# Create DAG

with DAG(
    'test_upload_gcs_to_bq',
    default_args=default_args,
    description='Test upload csv dat from gcs to bq',
    schedule_interval=None,
    tags=["Data Eng. project"]
) as dag:

# Tasks
    
    '''t0 = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id="anime_data",
        table_id="temp_table",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "rankId", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rank", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rankType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "format", "type": "STRING", "mode": "NULLABLE"},
            {"name": "rankYear", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "context", "type": "STRING", "mode": "NULLABLE"},
        ],        
    )'''
    
    
    t1 = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='anilist_fetch',
        source_objects=['merge/genres.csv'],
        destination_project_dataset_table='anime_data.temp_genres_table',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', #WRITE_EMPTY
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gen1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen6", "type": "STRING", "mode": "NULLABLE"},
        ],
    )


    # Dependencies
    t1