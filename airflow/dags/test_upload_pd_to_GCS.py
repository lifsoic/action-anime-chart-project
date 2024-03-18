from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import os

import pandas as pd

# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'Test_upload_df_csv_to_GCS',
    default_args=default_args,
    description='upload csv DataFrame to GCS',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

BUCKET_NAME = 'anilist_fetch'
OBJECT_NAME = 'create_folder/demo_df.csv'  # Replace with desired filename
GCS_CONN_ID = 'google_cloud_default'  # Set up this connection in Airflow UI
PROJECT_ID = 'anime-chart-data'  # Optional if authentication method requires it

# Only need this if you're running this code locally | GCS_key_path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/airflow/data/GCS_key/anime-chart-data-add71f57bddb.json'

def save_dataframe_to_gcs(bucket_name, object_name):
    """
    Saves a pandas DataFrame to a GCS bucket.

    Args:
        dataframe: The DataFrame to save.
        bucket_name: The name of the GCS bucket.
        object_name: The filename of the object to create.
        gcs_conn_id: The Airflow connection ID for Google Cloud Storage.
        project_id: The Google Cloud project ID (optional, depending on authentication).
    """
    dataframe = df_create()     # demo df

    #storage.Client()
    client = storage.Client()  # Pass
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)    

    # Convert DataFrame to CSV format
    csv_data = dataframe.to_csv(index=False).encode('utf-8')

    # Upload the CSV data to the GCS bucket
    blob.upload_from_string(csv_data)

    print(f"DataFrame saved to GCS bucket {bucket_name} as {object_name}")

def df_create():
    
    df = pd.DataFrame({'A':[1,2],'B':[3,4]})

    return df

t1 = PythonOperator(
    task_id='upload_df_csv_to_GCS',
    python_callable=save_dataframe_to_gcs,
    op_kwargs={
            "bucket_name" : BUCKET_NAME,
            "object_name" : OBJECT_NAME,
        },
    dag=dag,
)

# Dependencies

t1
