from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
#from dotenv import load_dotenv


import pandas as pd
import time
import os
import pymysql.cursors

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
    'test_connection_to_MySQL',
    default_args=default_args,
    description='Test conenction',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

#MySQL config
class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

def create_table():
    #connect to MySQL
    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                 port=Config.MYSQL_PORT,
                                 user=Config.MYSQL_USER,
                                 password=Config.MYSQL_PASSWORD,
                                 db=Config.MYSQL_DB,
                                 charset=Config.MYSQL_CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)
       
    with connection.cursor() as cursor:
        
        #crate table SQL command
        create_table_sql = """CREATE TABLE test_con3 (id INT, engTitle VARCHAR(255))"""
        
        cursor.execute(create_table_sql)

        #Commit the changes
        connection.commit()

        #Close the cursor
        cursor.close()                                                  
        #connection.close()

def print_config():
      
    print(f"Debug JAAAAAA !!!!! | {Config.MYSQL_HOST}")

#Tasks
t1 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

t0 = PythonOperator(
    task_id='print_config',
    python_callable=print_config,
    dag=dag,
) 

# Dependencies
t0 >> t1