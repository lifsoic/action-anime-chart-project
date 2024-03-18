from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'test_gcs_upload',
    default_args=default_args,
    description='DAG to upload a local file to Google Cloud Storage',
    schedule_interval=None,
    tags=["Data Eng. project"],
) as dag:

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_local_file",
        src="/home/airflow/data/Subject1.csv",  # Path to the file within the container | beware of this path
        dst="Subject1.csv",                    # Destination path in GCS | "anilist_fetch/Subject1.csv"
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )


    upload_file
