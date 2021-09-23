import json
import boto3
import pymysql
import logging
import csv

s3=boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

db_endpoint = 'DB_END_POINT'
db_user = 'db_User'
db_password = 'db_Password'
db_name = 'db_database'
port=3306

def check_file_exists(uploaded_filename_csv):
    try:
        uploaded_filename_csv=uploaded_filename_csv.replace(".csv","")
        connection=pymysql.connect(host=db_endpoint, user=db_user, password=db_password,database=db_name,port=port)    
        cursor = connection.cursor()
        #query="SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'demodatabase') AND (TABLE_NAME = '"+filename+"')"
        count_table = cursor.execute( "SHOW TABLES LIKE '%"+uploaded_filename_csv+"%'")
        return count_table
    except Exception as e:
        print('error')

def create_new_table(bucket_name,uploaded_filename_csv,table_schema):
    try:
        connection=pymysql.connect(host=db_endpoint, user=db_user, password=db_password,database=db_name,port=port)    
        cursor = connection.cursor()
        with open('/tmp/'+table_schema) as file:
            lines = file.readlines()
            lines=''.join(map(str, lines))
            lines=lines.replace("[","")
            lines=lines.replace("]","")
            lines=lines.replace("%20"," ")
            create_table_query = lines
            cursor.execute(create_table_query)
            connection.commit()
            logger.info('Table Created in RDS')
            insert_into_RDS(bucket_name,uploaded_filename_csv,table_schema)
    except Exception as e:
        logger.info('Table Creation Failed !!')
        logger.info(e) 

def insert_into_RDS(bucket_name,uploaded_filename_csv,table_schema):
        connection = pymysql.connect(host=db_endpoint,user=db_user,password=db_password,db=db_name,port=port)
        cursor = connection.cursor()
        uploaded_filename_csv=uploaded_filename_csv.replace(".csv","")
        cursor.execute("select column_name from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='"+uploaded_filename_csv+"'")
        column_names = cursor.fetchall()
        column_names = str(column_names)
        column_names=column_names.replace("(''","")
        column_names=column_names.replace("',)","")
        column_names=column_names.replace("('","")
        column_names=column_names.replace("',","")
        query = 'INSERT INTO '+uploaded_filename_csv+column_names+' VALUES(%s, %s, %s)'                                                         
        my_data = []
        with open('/tmp/'+uploaded_filename_csv+'.csv') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                        my_data.append(tuple(row))
        try:
                my_data.pop(0)
                cursor.executemany(query,my_data)
                connection.commit()
                print('commit')
        except:
                connection.rollback()
                print('rollback done')
        connection.close()
        
def lambda_handler(event,context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    uploaded_filename_csv = event['Records'][0]['s3']['object']['key']
    table_schema = uploaded_filename_csv
    table_schema=table_schema.replace(".csv",".json")
    s3.download_file(bucket_name,uploaded_filename_csv,"/tmp/"+uploaded_filename_csv)
    s3.download_file(bucket_name,table_schema,"/tmp/"+table_schema)
    count_table=check_file_exists(uploaded_filename_csv)
    if count_table == 1:
        insert_into_RDS(bucket_name,uploaded_filename_csv,table_schema)
    else:
        create_new_table(bucket_name,uploaded_filename_csv,table_schema)
