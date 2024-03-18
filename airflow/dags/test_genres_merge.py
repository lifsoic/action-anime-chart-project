from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from google.cloud import storage

from airflow.operators.dummy_operator import DummyOperator      # for demo only

import requests
import pandas as pd
import time
import os
import shutil

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

dag = DAG(
    'Test_genres_merge',
    default_args=default_args,
    description='test script of genres merged to GCS and BigQuery for check memory',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

# Config
temp_path = '/home/airflow/data/temp/'                      # temporary results path folder
main_path = f'{temp_path}main/loop/'                         # result of all main fetch files
tags_path = f'{temp_path}tags/loop/'                         # results path of tags fecth files
rankings_path = f'{temp_path}rankings/loop/'                     # results path of rankings  fecth files
genres_path = f'{temp_path}genres/loop/'                     # results path of genres  fecth files
path_list = [temp_path, main_path, tags_path, rankings_path, genres_path]
MYSQL_CONNECTION = "mysql_default"                          # MySQL connection id from airflow connection


def concatenate_files(source_path, destination_path, merge_file_name):
    files = os.listdir(source_path)                 #list file inside path
    first_file = True                               #Assign first file

    with open(os.path.join(destination_path, merge_file_name), "w") as output_file:     #open with 
        for file in files:
            with open(os.path.join(source_path, file), "r") as input_file:
                lines = input_file.readlines()      # Read all lines into a list

                if first_file:
                    # Write the header only for the first file
                    output_file.writelines(lines[0:])  # Write from the first line (header)
                    first_file = False
                else:
                    # Skip the header for subsequent files
                    output_file.writelines(lines[1:])  # Write from the second line onwards
    
    #sort id from output file
    read_merge = pd.read_csv(f'{destination_path}{merge_file_name}')
    read_merge = read_merge.sort_values('id', ascending= True)

    read_merge.to_csv(f'{destination_path}{merge_file_name}', index=False)

    # log for schema
    print(read_merge.info())

def join_data_from_mysql():
    
    # connect with MySQL
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # get engTitle table from MySQL
    get_eng = mysqlserver.get_pandas_df(sql="SELECT * FROM engTitle")    

    # get main df from temp folder
    main_df = pd.read_csv(f'{temp_path}/main/main_df.csv')

    # join engTitle and main_df with id
    merge_df = main_df.merge(get_eng, how='left', on='id')

    # save to replace main_df
    merge_df.to_csv(f'{temp_path}/main/main_df.csv', index=False)

    # log for schema check
    print(merge_df.info())

# Tasks
t6 = PythonOperator(
    task_id='merge_genres',
    python_callable=concatenate_files,
    op_kwargs={
            'source_path' : genres_path,
            'destination_path' : f'{temp_path}/genres/',
            'merge_file_name' : 'genres_df.csv',
        },
    dag=dag,
) 


'''t11 = LocalFilesystemToGCSOperator( 
        task_id="upload_genres_to_GCS",
        src=f'{temp_path}/genres/genres_df.csv',               # Path to the desire file
        dst="merge/genres.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )

t15 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_genres_to_GCS",
        src = f'{genres_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/genres/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t19 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_genres',
        bucket='anilist_fetch',
        source_objects=['merge/genres.csv'],
        destination_project_dataset_table='anime_data.genres',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', #'WRITE_EMPTY'
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gen1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gen6", "type": "STRING", "mode": "NULLABLE"},
        ],
        dag=dag,
    )'''


# Dependencies

#t6 >> t11 >> t15 >> t19
t6
