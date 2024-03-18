from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
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
    'depends_on_past': False,
    'catchup' : False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Create DAG

dag = DAG(
    'daily_score_fecth',
    default_args=default_args,
    description='Fecth score Data from Anilist database API daily',
    schedule_interval=None,
    tags=["Data Eng. project"]
)

# Config
temp_path = '/home/airflow/data/temp/'                      # temporary results path folder
main_path = f'{temp_path}main/loop/'                         # result of all main fetch files
tags_path = f'{temp_path}tags/loop/'                         # results path of tags fecth files
rankings_path = f'{temp_path}rankings/loop/'                     # results path of rankings  fecth files
path_list = [temp_path, main_path, tags_path, rankings_path]

def query_anime(startPage, main_path, tags_path, rankings_path):
    
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
                averageScore        # A weighted average score of all the user's scores of the media
                popularity          # The number of users with the media on their list
                favourites          # The amount of user's who have favourited the media
                rankings            # The ranking of a media in a particular time span and format compared to other media
                {          
                    id              # The id of the rank
                    rank            # The numerical rank of the media
                    type            # RATED:Ranking is based on the media's ratings/score | POPULAR:Ranking is based on the media's popularity
                    format          # The format the media is ranked within
                    year            # The year the media is ranked within                
                    context         # String that gives context to the ranking type and time span
                }
                trending            # The amount of related activity in the past hour
                tags                # List of tags that describes elements and themes of the media
                {
                    id              # The id of the tag
                    name            # The name of the tag
                    description     # A general description of the tag
                    category        # The categories of tags this tag belongs to
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
    print(f'Page : {startPage} - Status code : {r.status_code}')
    results = r.json()    
    get_res = results["data"]["Page"]["media"]
    result_df = pd.DataFrame(get_res)           #main df

    # Separate tags column
    get_res_tags = pd.DataFrame(result_df['tags'].explode())    # explode(): list to dict form

    # Separate each dict key to column
    get_res_tags = pd.concat([get_res_tags.drop('tags', axis=1), get_res_tags['tags'].apply(pd.Series)], axis=1)

    # change col name for not duplicate with main df
    get_res_tags['tagId'] = get_res_tags['id']
    get_res_tags['tagName'] = get_res_tags['name']
    # clean some white space of some row
    get_res_tags['tagDescription'] = get_res_tags.apply(lambda x : x['description'].rstrip() if isinstance(x['description'], str) else x['description'], axis=1)
    get_res_tags['tagCategory'] = get_res_tags['category']
    get_res_tags = get_res_tags.drop(columns=['id','name','description','category'])

    # choose column only tagId, tagName, tagDescription, tagCategory
    get_res_tags = get_res_tags[['tagId', 'tagName', 'tagDescription', 'tagCategory']] 
    
    tag_df = result_df[['id']]                          # copy id from main df
    tag_df = tag_df.join(get_res_tags)                  # join index from separate df for separate table
    print('------- tag_df Null check ------------')
    print(tag_df.isnull().sum())                                # log print for data monitoring 

    # save tag_df to csv file to any path
    page_id = startPage                                 # page_id var
    tags_path = tags_path                             # ref. to tags_path var
    tag_file_name = f'{page_id}_tag_df.csv'              # tag_df.csv file name
    tag_df.to_csv(f"{tags_path}{tag_file_name}", index=False)

    # for check data type
    print(tag_df.info()) 

    # Separate ranking column
    get_res_rankings = pd.DataFrame(result_df['rankings'].explode())   # explode(): list to dict form

    # Separate each dict key to column
    get_res_rankings = pd.concat([get_res_rankings.drop('rankings', axis=1), get_res_rankings['rankings'].apply(pd.Series)], axis=1)

    # change col name for not duplicate with main df
    get_res_rankings['rankId'] = get_res_rankings['id']
    get_res_rankings['rankType'] = get_res_rankings['type']
    get_res_rankings['rankYear'] = get_res_rankings['year']
    get_res_rankings = get_res_rankings.drop(columns=['id', 'type', 'year'])
    #relocate columns for easy reading
    new_col_rankings = ['rankId', 'rank', 'rankType', 'format','rankYear', 'context']         
    get_res_rankings = get_res_rankings[new_col_rankings]

    rankings_df = result_df[['id']]                     # copy id from main df
    rankings_df = rankings_df.join(get_res_rankings)    # join index from separate df for separate table

    print('------- rankings_df Null check ------------')
    print(rankings_df.isnull().sum())                                  # log print for data monitoring

    # save rankings_df to csv file to any path
    rankings_path = rankings_path                # congfig any path
    rankings_file_name = f'{page_id}_rankings_df.csv'               # rankings_df.csv file name
    rankings_df.to_csv(f'{rankings_path}{rankings_file_name}', index=False)        #rankings_df.csv file name

    # drop raw column
    result_df = result_df.drop(columns=['rankings','tags'], axis=1)
    
    # Relocate column for easy reading
    result_df = result_df[['id', 'averageScore', 'popularity', 'favourites', 'trending']]
    
    # For data montoring info
    print('-----------------------------------')
    print(f' Page = {startPage} : Null check')
    print('-----------------------------------')
    print(result_df.isnull().sum())
    print('-----------------------------------')
    
    #save result_df to csv file to any path
    result_file_name = f'{page_id}_result_df.csv'                      
    result_df.to_csv(f"{main_path}{result_file_name}", index=False)

    check_hasNextPage = results['data']['Page']['pageInfo']['hasNextPage']  # Add  for check hasNextPage == False

    # For check data type
    print(result_df.info())

    return check_hasNextPage

def loop_fetch(main_path, tags_path, rankings_path):
    
    # Config
    get_hasNextPage = True                                      # For default start
    startPage = 1                                               # Customize your first page
    api_lim = 25                                                # api request call limit (30), 1 page per 1 request
    api_last_page = 200                                           # customize your last page 

    while get_hasNextPage == True:                              # break when found last page after api_last_page
        
        get_hasNextPage = query_anime(startPage, main_path, tags_path, rankings_path) # assign get_hasNextPage from return results
        
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

    # log for schema
    print(read_merge.info())


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
            'tags_path' : tags_path,
            'rankings_path' : rankings_path,
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
    task_id='merge_tags',
    python_callable=concatenate_files,
    op_kwargs={
            'source_path' : tags_path,
            'destination_path' : f'{temp_path}/tags/',
            'merge_file_name' : 'tags_df.csv',
        },
    dag=dag,
)

t5 = PythonOperator(
    task_id='merge_rankings',
    python_callable=concatenate_files,
    op_kwargs={
            'source_path' : rankings_path,
            'destination_path' : f'{temp_path}/rankings/',
            'merge_file_name' : 'ranking_df.csv',
        },
    dag=dag,
)

t6 = LocalFilesystemToGCSOperator(
        task_id="upload_main_to_GCS",
        src=f'{temp_path}/main/main_df.csv',               # Path to the desire file
        dst="merge/main_score.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )

t7 = LocalFilesystemToGCSOperator(
        task_id="upload_tags_to_GCS",
        src=f'{temp_path}/tags/tags_df.csv',               # Path to the desire file
        dst="merge/tags.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )

t8 = LocalFilesystemToGCSOperator(
        task_id="upload_rankings_to_GCS",
        src=f'{temp_path}/rankings/ranking_df.csv',               # Path to the desire file
        dst="merge/rankings.csv",                     # Destination path and name in GCS
        bucket="anilist_fetch",                 # bucket name
        dag = dag,
    )


t9 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_main_to_GCS",
        src = f'{main_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/score/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t10 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_tags_to_GCS",
        src = f'{tags_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/tags/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t11 = LocalFilesystemToGCSOperator(
        task_id ="upload_loop_rankings_to_GCS",
        src = f'{rankings_path}/*.csv',               # *.csv for all csv file in this path
        dst = "loop/rankings/",                     # Destination path and name in GCS
        bucket = "anilist_fetch",                 # bucket name
        dag = dag,
    )

t12 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_main',
        bucket='anilist_fetch',
        source_objects=['merge/main_score.csv'],
        destination_project_dataset_table='anime_data.animeScore',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=[
            {"name": "id","type": "INTEGER","mode": "NULLABLE"},
            {"name": "averageScore", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "popularity", "type": "FLOAT" , "mode": "NULLABLE"},
            {"name": "favourites", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "trending", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        dag=dag,
    )

t13 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_tags',
        bucket='anilist_fetch',
        source_objects=['merge/tags.csv'],
        destination_project_dataset_table='anime_data.tags',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "tagId", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tagName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tagDescription", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tagCategory", "type": "STRING", "mode": "NULLABLE"},
        ],
        dag=dag,
    )

t14 = GCSToBigQueryOperator(
        task_id='gcs_to_bq_rankings',
        bucket='anilist_fetch',
        source_objects=['merge/rankings.csv'],
        destination_project_dataset_table='anime_data.rankings',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "rankId", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rank", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rankType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "format", "type": "STRING", "mode": "NULLABLE"},
            {"name": "rankYear", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "context", "type": "STRING", "mode": "NULLABLE"},
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

t1 >> t2
t2 >> [t3, t4, t5]
t3 >> t6 >> t9 >> t12
t4 >> t7 >> t10 >> t13
t5 >> t8 >> t11 >> t14
[t12, t13, t14] >> del_temp