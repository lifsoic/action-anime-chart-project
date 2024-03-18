from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import os


# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG(
    'Test_create_temp',
    default_args=default_args,
    description='Create temporary folder for API fetch file',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

#Config
path = '/home/airflow/data/temp/test_folder'      #config any path

def create_temp (folder_path):
    os.makedirs(folder_path, exist_ok=True)
    
# Tasks

t1 = PythonOperator(
    task_id='Test_create_temp',
    python_callable=create_temp,
    op_kwargs={
            "folder_path": path,
        },
    dag=dag,
)


# Dependencies

t1