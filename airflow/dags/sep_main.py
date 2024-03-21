from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import requests
import pandas as pd
import time
import os
import shutil

# Default Args

default_args = {
    'owner': 'Alif',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval' : None,
}

# Create DAG

dag = DAG(
    'Separate_main_fetch',
    default_args=default_args,
    description='Fecth Data with no any score',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

# Config
temp_path = '/home/airflow/data/temp/'                      # temporary results path folder
main_path = f'{temp_path}main/loop/'                         # result of all main fetch files
genres_path = f'{temp_path}genres/loop/'                     # results path of genres  fecth files
path_list = [temp_path, main_path, genres_path]
MYSQL_CONNECTION = "mysql_default"                          # MySQL connection id from airflow connection

def query_anime(startPage, main_path, genres_path):
    
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
                    romaji          # The romanization of the native language title
                }
                episodes            # The amount of episodes the anime has when complete
                genres              # The genres of the media
                startDate           # The first official release date of the media
                {         
                    year
                    month
                    day
                }
                endDate             # The last official release date of the media
                {           
                    year
                    month
                    day
                }
                seasonYear          # The season year the media was initially released in
                duration            # The general length of each anime episode in minutes
                season              # The season the media was initially released in
                seasonInt           # The year & season the media was initially released in
                status              # The current releasing status of the media
                source              # Source type the media was adapted from                
                isAdult             # If the media is intended only for 18+ adult audiences
                siteUrl             # The url for the media page on the AniList website
                countryOfOrigin     # Where the media was created. (ISO 3166-1 alpha-2)                
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
    print(f'Page : {startPage} - Status code : {r.status_code}')
    results = r.json()    
    get_res = results["data"]["Page"]["media"]
    result_df = pd.DataFrame(get_res)           #main df

    # Separate Title list of dict form to column
    get_res_title = pd.DataFrame(result_df["title"])

    # split romaji into column
    get_res_title = get_res_title['title'].apply(pd.Series)

    # change col name and drop current col
    get_res_title['romajiTitle'] = get_res_title['romaji']
    result_df = result_df.join(get_res_title.drop(columns=['romaji']))

    # Separate startDate format to column
    get_res_startdate = pd.DataFrame(result_df['startDate'])

    # split from dict to date format
    st_date = []
    for row in get_res_startdate['startDate']:        
        year = row['year']
        month = row['month']
        day = row['day']
        startDate = pd.to_datetime(f'{year}-{month}-{day}', errors='coerce', yearfirst=True) # date format, if error ruturn 'NaT'
        st_date.append(startDate)
    get_res_startdate['startDate'] = st_date

    # Separate endDate format to column
    get_res_endDate = pd.DataFrame(result_df['endDate'])

    # split from dict to date format
    end_date = []
    for row in get_res_endDate['endDate']:        
        year = row['year']
        month = row['month']
        day = row['day']
        endDate = pd.to_datetime(f'{year}-{month}-{day}', errors ='coerce', yearfirst=True) # date format, if error rutrn 'NaT'
        end_date.append(endDate)
    get_res_endDate['endDate'] = end_date

    # replace 'startDate' & 'endDate' df to main df
    result_df = result_df.drop(columns=['startDate', 'endDate'])    # raw startDate' & 'endDate'
    result_df['startDate'] = get_res_startdate
    result_df['endDate'] = get_res_endDate    

    # Separate each list to row to create new df of genres
    get_genres = pd.DataFrame(result_df['genres'].explode())

    # Merge with index from separate df for separate table
    genres_df = result_df[['id']]    
    genres_df = genres_df.join(get_genres)                
    print('------- genres_df Null check ------------')
    print(get_genres.isnull().sum())                                 # log print for data monitoring

    # save genres_df to csv file to any path
    page_id = startPage                                 # page_id var
    genres_path = genres_path                                       #congfig any path
    genres_file_name = f'{page_id}_genres_df.csv'                  #genres_df.csv file name
    genres_df.to_csv(f"{genres_path}{genres_file_name}", index=False)

    # for check data type
    print(genres_df.info())

    # drop raw column
    result_df = result_df.drop(columns=['title','genres'], axis=1)
        
    # Relocate column for easy reading
    result_df = result_df[['id','romajiTitle', 'startDate', 'endDate', 'episodes', 'seasonYear', 'duration', 'season', 'seasonInt', 'status','source', 'isAdult', 'siteUrl', 'countryOfOrigin']]
    
    # For data montoring info
    print('-----------------------------------')
    print(f' Page = {startPage} : Null check')
    print('-----------------------------------')
    print(result_df.isnull().sum())
    print('-----------------------------------')
    
    #save result_df to csv file to any path
    result_file_name = f'{page_id}_result_df.csv'                      # result_df.csv file name
    result_df.to_csv(f"{main_path}{result_file_name}", index=False)

    check_hasNextPage = results['data']['Page']['pageInfo']['hasNextPage']  # Add  for check hasNextPage == False

    # For check data type
    print(result_df.info())

    return check_hasNextPage

def loop_fetch(main_path, genres_path):
    
    # Config
    get_hasNextPage = True                                      # For default start
    startPage = 1                                               # Customize your first page
    api_lim = 25                                                # api request call limit (30), 1 page per 1 request
    api_last_page = 200                                           # customize your last page 

    while get_hasNextPage == True:                              # break when found last page after api_last_page
        
        get_hasNextPage = query_anime(startPage, main_path, genres_path) # assign get_hasNextPage from return results
        
        startPage += 1                                                      # assign next page    
        
        if (startPage == api_lim) or (startPage%api_lim == 0 ):             
            print(f'Take delay for 1 min')                                  
            time.sleep(60)                                                  #delay 60s (1 min) for make sure not over api limit (30 request/min)
            print(f'Continue fetch after delay')

        elif startPage > api_last_page:                                     #stop at api_last_page fetch
            print('Too much fetch !!!')
            break
    
    print("----------------------------------- Complete query :)")

def create_temp(path_list):
    
    for i in path_list:
        os.makedirs(i, exist_ok=True)

        print(f'Directory : {i} has been created')

def delete_temp(path):
    
    shutil.rmtree(path)

    print(f'All files in {path} was deleted')

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


def join_data_from_mysql():
    
    # connect with MySQL
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # get engTitle table from MySQL
    get_eng = mysqlserver.get_pandas_df(sql="SELECT * FROM engTitle")    

    # get main df from temp folder
    main_df = pd.read_csv(f'{temp_path}/main/main_df.csv')

    # join engTitle and main_df with id
    merge_df = main_df.merge(get_eng, how='left', on='id')
    
    # crate title column and use engTitle at primary value
    merge_df['title'] = merge_df['engTitle'].fillna(merge_df['romajiTitle'])

    # save to replace main_df
    merge_df.to_csv(f'{temp_path}/main/main_df.csv', index=False)

    # log for schema check
    print(merge_df.info())


#Tasks

t1 = PythonOperator(
    task_id='create_temp_tree_folder',
    python_callable=create_temp,
    op_kwargs={
            "path_list" : path_list,
        },
    dag=dag,
)

t2 = PythonOperator(
    task_id='loop_query_anime_data',
    python_callable=loop_fetch,
    op_kwargs={
            'main_path' : main_path,
            'genres_path' : genres_path,            
        },
    dag=dag,
)

t3 = PythonOperator(
    task_id='merge_main',
    python_callable=concatenate_files,
    op_kwargs={
            'source_path' : main_path,
            'destination_path' : f'{temp_path}/main/',
            'merge_file_name' : 'main_df.csv',
        },
    dag=dag,
)

t4 = PythonOperator(
    task_id='merge_genres',
    python_callable=concatenate_files,
    op_kwargs={
            'source_path' : genres_path,
            'destination_path' : f'{temp_path}/genres/',
            'merge_file_name' : 'genres_df.csv',
        },
    dag=dag,
) 

t5 = PythonOperator(
    task_id='hook_and_join_from_MySQL',
    python_callable=join_data_from_mysql,
    dag=dag,
)

t6 = LocalFilesystemToGCSOperator(
        task_id="upload_main_to_GCS",
        src=f'{temp_path}/main/main_df.csv',               # Path to the desire file
        dst="merge/main.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )

t7 = LocalFilesystemToGCSOperator( 
        task_id="upload_genres_to_GCS",
        src=f'{temp_path}/genres/genres_df.csv',               # Path to the desire file
        dst="merge/genres.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )

t8 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_main_to_GCS",
        src = f'{main_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/main/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t9 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_genres_to_GCS",
        src = f'{genres_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/genres/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t10 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_main',
        bucket='anilist_fetch',
        source_objects=['merge/main.csv'],
        destination_project_dataset_table='anime_data.mainAnime',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=[
            {"name": "id","type": "INTEGER","mode": "NULLABLE"},
            {"name": "romajiTitle","type": "STRING", "mode": "NULLABLE"},
            {"name": "startDate", "type": "DATE", "mode": "NULLABLE"},
            {"name": "endDate", "type": "DATE", "mode": "NULLABLE"},
            {"name": "episodes", "type": "FLOAT", "mode": "NULLABLE",},
            {"name": "seasonYear", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "duration","type": "FLOAT", "mode": "NULLABLE"},
            {"name": "season","type": "STRING", "mode": "NULLABLE"},
            {"name": "seasonInt", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "status","type": "STRING", "mode": "NULLABLE"},
            {"name": "source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "isAdult", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "siteUrl", "type": "STRING", "mode": "NULLABLE"},
            {"name": "countryOfOrigin", "type": "STRING", "mode": "NULLABLE"},
            {"name": "engTitle", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        ],
        dag=dag,
    )

t11 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_genres',
        bucket='anilist_fetch',
        source_objects=['merge/genres.csv'],
        destination_project_dataset_table='anime_data.genres',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "genres", "type": "STRING", "mode": "NULLABLE"},
        ],
        dag=dag,
    )

del_temp = PythonOperator(
    task_id='del_temp',
    python_callable=delete_temp,
    op_kwargs = {
        'path' : temp_path,
    },      
    dag=dag,
)


# Dependencies

t1 >> t2 >> [t3, t4]
t3 >> t5 >> t6 >> t8 >> t10
t4 >> t7 >> t9 >> t11
[t10, t11] >> del_temp