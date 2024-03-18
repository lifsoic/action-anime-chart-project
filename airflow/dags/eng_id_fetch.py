from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import requests
import pandas as pd
import time
import os
import pymysql

# Default Args

default_args = {
    'owner': 'Alif',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Create DAG

dag = DAG(
    'ID_and_English_title_fetch',
    default_args=default_args,
    description='Fecth Data from Anilist database API and upload to MySQL',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

# Function config
get_hasNextPage = True                                  # For default start
startPage = 1                                           # Customize your first page
api_lim = 25                                            # api request call limit (30), 1 page per 1 request
api_last_page = 200                                     # customize your last page 
result_path = '/home/airflow/data/loop_id/'             # result of all fetch files
source_path = result_path                               # source of merge file funtion 
destination_path = '/home/airflow/data/'                # merge file location
merge_file_name = 'merge_id.csv'                        # merge file name
table_name = 'engTitle'                                 # assign table name

#MySQL config
class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

def query_anime(startPage, result_path):
    query ='''
    query ($id: Int, $page: Int, $perPage: Int, $search: String) {
        Page (page: $page, perPage: $perPage) {
            pageInfo {
                total
                currentPage
                lastPage
                hasNextPage
                perPage
            }
            media (id: $id, search: $search, genre: "Action", type: ANIME) {
                id
                title
                {
                english            #The official english title
                }
            }
        }
    }
    '''
    variables = {
        'page': startPage,
        'perPage': 50,     #Max at 50
    }

    url = 'https://graphql.anilist.co'
    r = requests.post(url, json={'query': query, 'variables': variables})
    print(f'Status code : {r.status_code}')
    results = r.json()
    
    get_res = results["data"]["Page"]["media"]
    result_df = pd.DataFrame(get_res)

    #Separate Title list of dict form to column
    get_res_title = pd.DataFrame(result_df["title"])
    get_res_title = get_res_title['title'].apply(pd.Series)     #split title into column
    get_res_title['engTitle'] = get_res_title['english']

    #clean column
    result_df = result_df.join(get_res_title.drop(columns=['english']))
    result_df = result_df.drop(columns=['title'], axis=1)

    print('-----------------------------------')
    print(f' Page = {startPage} : Null check')
    print('-----------------------------------')
    print(result_df.isnull().sum())
    print('-----------------------------------')

    #save result_df to csv file to any path
    page_id = startPage                                                #page_id var
    result_file_name = f'{page_id}_result_df.csv'                      #result_df.csv file name
    result_df.to_csv(f"{result_path}{result_file_name}", index=False)

    check_hasNextPage = results['data']['Page']['pageInfo']['hasNextPage']  #Add  for check hasNextPage == False

    return check_hasNextPage, r.status_code

def loop_fetch(get_hasNextPage, startPage, api_lim, api_last_page, result_path):
    while get_hasNextPage == True:                                      #break when found last page after api_last_page
        get_hasNextPage, res_code = query_anime(startPage, result_path)     #assign get_hasNextPage from return results
        startPage += 1                                                      #assign next page    
        
        if (startPage == api_lim) or (startPage%api_lim == 0 ):             
            print(f'Take delay for 1 min')                        
            time.sleep(60)                                                  #delay 60s (1 min) for make sure not over api limit (30 request/min)
            print(f'Continue fetch after delay')

        elif startPage > api_last_page:                                     #stop at api_last_page fetch
            print('Too much fetch !!!')
            print('Check query filter !!!')
            break
    
    print("----------------------------------- Complete query :)")

def concatenate_and_transfer_files(source_path, destination_path, merge_file_name):
    files = os.listdir(source_path)                 #list file inside path
    first_file = True                               #Flag to track the first file

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
    
    #sort from output file
    read_merge = pd.read_csv(f'{destination_path}{merge_file_name}')
    read_merge = read_merge.sort_values('id',ascending= True)
    read_merge.to_csv(f'{destination_path}{merge_file_name}', index=False)

def insert_data_to_mysql(destination_path, merge_file_name, table_name):
    
    #connect to MySQL | need config after 
    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                 port=Config.MYSQL_PORT,
                                 user=Config.MYSQL_USER,
                                 password=Config.MYSQL_PASSWORD,
                                 db=Config.MYSQL_DB,
                                 charset=Config.MYSQL_CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)
    
    with connection.cursor() as cursor:
              
        #load data to table
               
        #data input preparation
        df = pd.read_csv(f'{destination_path}{merge_file_name}')
        data = df.where((pd.notnull(df)), None)     # Change nan to None | Replace values where the condition is False.
        data = data.values.tolist()                 # Convert DataFrame to list of tuples

        insert_sql = f"""INSERT INTO {table_name} (id, engTitle) VALUES (%s, %s)"""

        cursor.executemany(insert_sql, data)

        connection.commit()
        cursor.close()        


# Tasks

t1 = PythonOperator(
    task_id='Loop_query_id_and_title',
    python_callable=loop_fetch,
    op_kwargs={
            "get_hasNextPage": get_hasNextPage,
            "startPage": startPage,
            "api_lim" : api_lim,
            "api_last_page" : api_last_page,
            "result_path" : result_path,
        },
    dag=dag,
)

t2 = PythonOperator(
    task_id='merge_fetched_file',
    python_callable=concatenate_and_transfer_files,
    op_kwargs={
            "source_path": source_path,
            "destination_path": destination_path,
            "merge_file_name" : merge_file_name,
        },
    dag=dag,
)

t3 = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql_con',          # Replace with your MySQL connection ID
    sql=f"""CREATE TABLE IF NOT EXISTS {table_name} (id INT, engTitle VARCHAR(255))""",
    dag=dag,
)

t4 = PythonOperator(
    task_id='insert_data_to_MySQL',
    python_callable=insert_data_to_mysql,
    op_kwargs={
            "destination_path": destination_path,
            "merge_file_name" : merge_file_name,
            "table_name" : table_name,
        },
    dag=dag,
)


# Dependencies

[t3, t1 >> t2] >> t4
