import csv, ast, psycopg2
import os
from operator import contains
from dotenv import load_dotenv

load_dotenv()

def build_temp_table_query(csvfilepath, vfilelist):
    filepath = open(csvfilepath, 'r')
    reader = csv.reader(filepath)
    headers = []
    tablename = 'temp_'+vfilelist[0][0]+'_table'
    for row in reader:
        if len(headers) == 0:
            headers = row
            statement = 'create temp table '+tablename+'('
            for i in headers:
                if 'personid' in i:
                    statement = (statement + '\n{} int8,').format(i.lower())
                else:
                    statement = (statement + '\n{} varchar({}),').format(i.lower(),256)
                #if 'date' in i:
                #    statement = (statement + '\n{} date,').format(i.lower())
            statement = statement[:-1] + ');'
    return statement, tablename

def build_copy_redshift_query(csvfilepath, s3path, vfilelist):
    filepath = open(csvfilepath, 'r')
    reader = csv.reader(filepath)
    headers = []
    for row in reader:
        if len(headers) == 0:
            headers = row
            statement = 'COPY temp_'+vfilelist[0][0]+'_table('
            for i in headers:
                statement = (statement + "{},").format(i.lower())
    statement = statement[:-1] + ")FROM '" + str(s3path) + "' CREDENTIALS 'aws_access_key_id=" + os.getenv('ACCESS_KEY') + ";aws_secret_access_key=" + os.getenv('SECRET_KEY')
    statement = statement + "' FILLRECORD DELIMITER ',' IGNOREHEADER 1 DATEFORMAT AS 'DD/MM/YYYY';"
    return statement 
