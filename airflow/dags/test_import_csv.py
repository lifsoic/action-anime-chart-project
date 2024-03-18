from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
import pymysql


# Default Args

default_args = {
    'owner': 'Alif',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Create DAG

dag = DAG(
    'test_import_csv_to_MySQL',
    default_args=default_args,
    description='Import local volume in airflow',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

#Connection config
csv_file_path = '/home/airflow/data/merge_id.csv'  # Replace with your CSV file path
table_name = 'test_engTitle'  # Replace with your desired table name


#MySQL config
class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

def create_and_import_csv(csv_file_path, table_name):

    #connect to MySQL | need config after 
    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                 port=Config.MYSQL_PORT,
                                 user=Config.MYSQL_USER,
                                 password=Config.MYSQL_PASSWORD,
                                 db=Config.MYSQL_DB,
                                 charset=Config.MYSQL_CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)
    
    with connection.cursor() as cursor:
        
        df = pd.read_csv(csv_file_path)
        data = df.where((pd.notnull(df)), None)     # Change nan to None | Replace values where the condition is False.
        data = data.values.tolist()                 # Convert DataFrame to list of tuples
        
        sql = f"""INSERT INTO {table_name} (id, engTitle) VALUES (%s, %s)"""
    
        cursor.executemany(sql, data)
        
        connection.commit()        
        cursor.close()
       
# Tasks
        
t1 = PythonOperator(
    task_id='import_csv',
    python_callable=create_and_import_csv,
    op_kwargs={
            "csv_file_path": csv_file_path,
            "table_name" : table_name,
        },
    dag=dag,
)

# Dependencies

t1