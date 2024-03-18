from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import shutil

import os
import pandas as pd

# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG(
    'Test_create_delete_temp',
    default_args=default_args,
    description='Create, save and delete temp folder in local',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

temp_path = '/home/airflow/data/temp/'
main_path = f'{temp_path}main/loop'
tag_res_path = f'{temp_path}tags//loop'
rank_res_path = f'{temp_path}rankings/loop'
genres_res_path = f'{temp_path}genres/loop'
path_list = [temp_path, main_path, tag_res_path, rank_res_path, genres_res_path]

def create_temp(path_list):
    for i in path_list:
        os.makedirs(i, exist_ok=True)

def delete_temp(path):
    shutil.rmtree(path)
    
def df_create(path, file_name):
    
    df = pd.DataFrame({'A':[1,2],'B':[3,4]})
    df = df.to_csv(f'{path}{file_name}.csv', index=False)            #assign file name

#Tasks

t1 = PythonOperator(
    task_id='create_temp_tree_folder',
    python_callable=create_temp,
    op_kwargs={
            'path' : path_list,
        },
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_and_save_df',
    python_callable=df_create,
    op_kwargs={
            'path' : main_path,
            'file_name' : 'demo_filename'
        },
    dag=dag,
)

t3 = PythonOperator(
    task_id='delete_temp_tree_folder',
    python_callable=delete_temp,
    op_kwargs={
            'path' : temp_path,
        },
    dag=dag,
)

# Dependencies

t1 >> t2 >> t3
