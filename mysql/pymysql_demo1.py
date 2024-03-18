import pymysql.cursors
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

#MySQL config
'''MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Rse65t57$'
MYSQL_DB = 'alifdb2'
MYSQL_CHARSET = 'utf8mb4'
'''


def create_table():
    #connect to MySQL
    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                port=Config.MYSQL_PORT,
                                user=Config.MYSQL_USER,
                                password=Config.MYSQL_PASSWORD,
                                db=Config.MYSQL_DB,
                                charset=Config.MYSQL_CHARSET,
                                cursorclass=pymysql.cursors.DictCursor)
    
    '''connection = pymysql.connect(host=MYSQL_HOST,
                                port=MYSQL_PORT,
                                user=MYSQL_USER,
                                password=MYSQL_PASSWORD,
                                db=MYSQL_DB,
                                charset=MYSQL_CHARSET,
                                cursorclass=pymysql.cursors.DictCursor)'''    
  

    with connection.cursor() as cursor:
        
        #insert config of table
        table_name = 'table1'
        columns = {
                'id': 'VARCHAR(255)',           #id of anime
                'engTitle': 'VARCHAR(255)'       #English title of anime
        }

        #crate table SQL command
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join([f"{column_name} {column_type}" for column_name, column_type in columns.items()])}
                )"""
        
        cursor.execute(create_table_sql)

        #Commit the changes
        connection.commit()

        #Close the cursor and connection
        cursor.close()                                                  
        connection.close()        

create_table()

print(f'{Config.MYSQL_HOST}')
print(f'{Config.MYSQL_PORT}')
print(f'{Config.MYSQL_USER}')
print(f'{Config.MYSQL_PASSWORD}')
print(f'{Config.MYSQL_DB}')
print(f'{Config.MYSQL_CHARSET}')
