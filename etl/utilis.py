import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os
import boto3
from botocore.exceptions import ClientError
import sys
import logging 
from sqlalchemy import create_engine
logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv(find_dotenv())

#AWS 
region_name = os.getenv('AWS_REGION_NAME')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')

#DB
db_name =  os.getenv('DATABASE_NAME')
db_username =  os.getenv('DATABASE_USER_NAME')
db_pass =  os.getenv('DATABASE_PASSWORD')
db_host =  os.getenv('DATABASE_HOST')
db_table =  os.getenv('DATABASE_TABLE')

data = 'dataset.csv'

# Connect to DWH 
def connect_to_dwh(db_name, db_username, db_pass, db_host):
    try:
        log.info('Connecting to DWH')
        conn_string = f"mysql+mysqlconnector://{db_username}:{db_pass}@{db_host}/{db_name}"
        engine = create_engine(conn_string)
        global conn
        conn = engine.connect()

        log.info('Connection to DWH successful')
        return conn
    
    except Exception as e:
        log.info('Cannot connect to DWH ', e)
        raise e

# connect_to_dwh(db_name, db_username, db_password, db_host, db_port)

def copy_file_to_db(db_table, data):

    engine = connect_to_dwh(db_name, db_username, db_pass, db_host)

# Copy to DB directly
    try:
        log.info(f'Copying csv to: {db_table} table')
        loadToDB = pd.read_csv(data)
       
        #Copy file to db
        loadToDB.to_sql(db_table, con = conn, index=False, if_exists='replace')

        log.info(f'Data copied successfully: {loadToDB.shape[0]} rows')
    except Exception as e:
        log.info(f'Failed to copy to DB {e}')
        # raise e

#  Copy to redshift table from S3 bucket
    # try:
    #     log.info(f"Copying data from S3 object to Redshift table '{aws_redshift_table}'...")
    #     copy_query = f"""
    #         COPY {aws_redshift_schema}.{aws_redshift_table}
    #         FROM 's3://{aws_bucket_name}/'
    #         CREDENTIALS 'aws_iam_role={aws_role}'
    #         REGION '{region_name}'
    #         IGNOREHEADER 1
    #         CSV;

    #         COMMIT;
    #     """
    #     with engine.connect() as connection:
    #         connection.execute(copy_query)  
    #         log.info('Dataset uploaded to redshift successfully')         
    # except Exception as e:
    #     log.info("Couldn't copy file to redshift table ", e)
    #     #raise e

copy_file_to_db(db_table,data)