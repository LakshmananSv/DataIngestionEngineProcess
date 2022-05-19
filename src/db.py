from dataclasses import dataclass
import json
import os
import boto3
import psycopg2
import datetime as dt
from dotenv import load_dotenv
import redshift_connector

load_dotenv()  # take environment variables from .env.

# DB constants
POSTSQL_DATABASE = os.getenv('MYSQL_DATABASE')
POSTSQL_DB_SECRET_NAME = os.getenv('DB_SECRET_NAME')
REDSHIFT_DATABASE = os.getenv('REDSHIFTSQL_DATABASE')
REDSHIFT_DB_SECRET_NAME = os.getenv('REDSHIFT_SECRET_NAME')
DB_AWS_REGION = os.getenv("AWS_REGION")


@dataclass
class IngestionEngine:
    customer_id: str

    def __init__(self, customer_tuple):
        self.customer_id = customer_tuple[0]

def get_sql_connection(dflag):
    try:
        session = boto3.session.Session()
        client = session.client(service_name = 'secretsmanager', region_name = DB_AWS_REGION)
        if dflag == 0:
            secret_name = POSTSQL_DB_SECRET_NAME
            database_name = POSTSQL_DATABASE
            py_conn_name = psycopg2

        if dflag == 1:
            secret_name = REDSHIFT_DB_SECRET_NAME
            database_name = REDSHIFT_DATABASE
            py_conn_name = redshift_connector
        
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            db_connection = py_conn_name.connect(host=secret["host"],
                                            user=secret["username"],
                                            password=secret["password"],
                                            database=database_name)
        return db_connection
    except Exception as e:
        print(e)
        print("Exception occurred during DB connect, Unable to get secrets")
        return None

def get_customer_endpoint(connection, customer_id):
    with connection.cursor() as cursor:
        query = '''SELECT secret_name, db_name
                FROM public.customer_endpoint_control
                WHERE customer_identifier LIKE %s
                '''
        cursor.execute(query, (customer_id,))
        dbout = cursor.fetchall()
        dbresult = [item for t in dbout for item in t]
        return dbresult

def get_table_columns(connection, dataprovider, product):
    with connection.cursor() as cursor:
        query = '''SELECT mc.csv
                FROM data_provider_ingestion_map_control mc
                JOIN data_provider_ingestion_control dp ON dp.rid = mc.dataprovider
                JOIN data_provider_product_ingestion_control dpp ON dpp.rid = mc.product
                WHERE dp.data_provider_name = %s
                AND dpp.product_name = %s 
                '''
        cursor.execute(query, (dataprovider,product))
        dbout = cursor.fetchall()
        dbresult = [item for t in dbout for item in t]
        return dbresult

def execute_query(connection, query: str):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return True
    except Exception as e:
        print(e) 
    finally:
        #connection.rollback()
        connection.autocommit=True
    
def compare_uid_count(connection, tmptablename):
    with connection.cursor() as cursor:
        query0 = ('''SELECT count(recordid) FROM {}''').format(tmptablename)
        query1 = ('''SELECT count(*) FROM {} tmp JOIN uid_control uc ON uc.uid = tmp.recordid''').format(tmptablename)
        cursor.execute(query0)
        count0 = cursor.fetchone()
        print('Number of recordid from CSV', count0)
        cursor.execute(query1)
        count1 = cursor.fetchone()
        print('Number of recordid from both CSV and uid_control', count1)
        if count0 == count1:
            return True