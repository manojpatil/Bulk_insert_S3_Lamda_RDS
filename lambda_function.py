import json
import boto3
import csv
import mysql.connector
import os

s3 = boto3.client('s3')
rds_endpoint  = 'database-1.csc5q5bwthqk.ap-south-1.rds.amazonaws.com'
username = 'admin'  #username for RDS Mysql
password = 'password'  # RDS Mysql password
db_name = 'demodatabase'  # RDS MySQL DB name
conn = None

bucket=''
key=''
def lambda_handler(event,context):
        for record in event['Records']:
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key'] 
        
        db = mysql.connector.connect(   host = rds_endpoint,
                        user = username,
                        passwd = password,
                        db = db_name)
        cursor = db.cursor()

        query = 'INSERT INTO test(name,email,department) VALUES(%s, %s, %s)'                                                         
        my_data = []
        with open(key) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                        my_data.append(tuple(row))
        try:
                # Executing the SQL command
                cursor.executemany(query,my_data)
                # Commit your changes in the database
                db.commit()
                print('commit')

        except:
                # Rolling back in case of error
                db.rollback()
                print('rollback')

        print("Data inserted")

        # Closing the connection
        db.close()