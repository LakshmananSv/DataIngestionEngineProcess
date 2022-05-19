import os
from tkinter import E
import boto3
import botocore
from dotenv import load_dotenv
import dotenv
from db import *
from logger_report import LoggerReport
import glob
import re
from helpers import *
import csv
from pandas import *

load_dotenv() # take environment variables from .env.
dotenv_path = '.env'  

def download_directory_from_s3(bucketName, remoteDirectoryName, logger_report: LoggerReport):
    try:
        s3_resource = boto3.resource('s3', aws_access_key_id = os.getenv('ACCESS_KEY'), aws_secret_access_key = os.getenv('SECRET_KEY'))
        bucket = s3_resource.Bucket(bucketName) 
        for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
            if obj.key != remoteDirectoryName:
                if not os.path.exists(os.path.dirname(obj.key)):
                    os.makedirs(os.path.dirname(obj.key))
                bucket.download_file(obj.key, obj.key)
        s3uri = 's3://'+ bucketName + '/' + remoteDirectoryName
        return(os.path.abspath(remoteDirectoryName), s3uri)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            ie_error = f"There are no source files to process, hence terminating the program"
            logger_report.error(ie_error)
        else:
            raise

def csv_filename_check(directory_path, logger_report: LoggerReport):
    try:
        num_files = []
        filename = []
        directory_path = directory_path + '/*.csv'
        for name in glob.glob(directory_path):
            if re.match(r'^.[a-z]+_(?:dataondemand|readgroup|experian|equifax|transunion|gbg|lexisnexis|creditsafe)_(?:fintrace|idvu|residence)_\d{8}\.csv$', os.path.basename(name)):
                temp_path_list = os.path.basename(name)
                num_files.append(temp_path_list)
        for file in num_files:
            split_filename = file.split('_')[0:3]
            filename.append(split_filename)      
        return zip(filename, num_files)
    except Exception as e:
        logger_report.error(e)
        raise e

def csv_column_check(vfilelist, localfilepath, logger_report: LoggerReport):
    try:
        csvlocation = localfilepath + '/' + vfilelist[1]
        with get_sql_connection(0) as connection:
            dbcolumnlist = get_table_columns(connection, vfilelist[0][1], vfilelist[0][2])
        with open(csvlocation) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter = ',')
            vcolumnlist = []
            for col in csv_reader:
                vcolumnlist.append(col)
                break
        if vcolumnlist[0] == dbcolumnlist[:-1]:
            #print("Both csv columns (", len(vcolumnlist[0]), ") and db columns (", len(dbcolumnlist[:-1]), ") are same!")
            return True
    except Exception as e:
        logger_report.error(e)
        raise e        

def initialize_connection_to_endpoint(customerid):
    with get_sql_connection(0) as connection:
        secret_name = get_customer_endpoint(connection, customerid)
        dotenv.set_key(dotenv_path,'REDSHIFTSQL_DATABASE', secret_name[1])
        dotenv.set_key(dotenv_path,'REDSHIFT_SECRET_NAME', secret_name[0])

def create_temp_table(vfilelist, localfilepath, s3uri):
    try:
        initialize_connection_to_endpoint(vfilelist[0][0])
        csvlocation = localfilepath + '/' + vfilelist[1]
        dbquerytemptable,tmptablename = build_temp_table_query(csvlocation, vfilelist)
        s3uri = s3uri + vfilelist[1]
        loadcsvquery = build_copy_redshift_query(csvlocation, s3uri, vfilelist)
        with get_sql_connection(1) as connection:
            execute_query(connection, dbquerytemptable)
            execute_query(connection, loadcsvquery)
            c = compare_uid_count(connection, tmptablename)
            print('Compare UID Count Result', c)
    except Exception as e:
        logger_report.error(e)
        raise e

             
if __name__ == "__main__":
    logger_report = LoggerReport()
    bucketName = os.getenv('BUCKET_NAME')
    remoteDirectoryName = os.getenv('BUCKET_DIRECTORY')
    localfilepath, s3uri = download_directory_from_s3(bucketName, remoteDirectoryName, logger_report)
    verified_files = csv_filename_check(localfilepath, logger_report)
    for index in verified_files:
        print(index[1])
        compareresult = csv_column_check(index, localfilepath, logger_report)
        if compareresult:
            ttc_output = create_temp_table(index, localfilepath, s3uri)