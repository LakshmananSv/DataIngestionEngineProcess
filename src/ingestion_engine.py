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
from db_class import *
import csv
from pandas import *
from datetime import datetime
import collections


load_dotenv() # take environment variables from .env.
dotenv_path = '.env'  

def download_directory_from_s3(bucketName, remoteDirectoryName, logger_report: LoggerReport):
    try:
        print('Downloading all CSV files from S3 bucket' + bucketName)
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

def csv_normalise_mobile_num(vfilelist, directory_path, bucketName, remoteDirectoryName):
    try:
        print('Normalzing phone numbers and date of birth for the CSV file' + vfilelist[1])
        csvlocation = directory_path + '/' + vfilelist[1]
        df = pd.read_csv(csvlocation, dtype="str")
        columnshdrlist=df.columns.values.tolist()
        mobilematching = [s for s in columnshdrlist if "Mobile" in s and "Date" not in s]
        landlinematching = [s for s in columnshdrlist if "Landline" in s and "Date" not in s]
        combinedlist = mobilematching + landlinematching
        for item in combinedlist:
            for i in range(0, len(df)):
                x = str(df.iloc[i][item])
                if x!="nan":
                     y = x.replace(" ", "")
                     df.loc[i, item] = "0" + y[-10:]
        datematching = [s for s in columnshdrlist if "dateofbirth" in s]
        for dateitem in datematching:
            df[dateitem] = pd.to_datetime(df[dateitem], errors = 'coerce' )
            df[dateitem] = df[dateitem].dt.strftime('%Y-%m-%d')
        df.to_csv(csvlocation, index=False)
        print('Re uploading CSV files to the S3 bucket' + bucketName + 'for temporary table creation' + vfilelist[1])
        s3_resource = boto3.resource('s3', aws_access_key_id = os.getenv('ACCESS_KEY'), aws_secret_access_key = os.getenv('SECRET_KEY'))
        s3_resource.meta.client.upload_file(Filename = csvlocation, Bucket = bucketName, Key = remoteDirectoryName + vfilelist[1])
    except Exception as e:
        logger_report.error(e)
        raise e

def csv_filename_check(directory_path, logger_report: LoggerReport):
    try:
        num_files = []
        filename = []
        print('Verifying the incoming CSV file has correct filename convention for all downloaded files')
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
        print('Comparing the CSV columns with data_ingestion_control table location match columns')
        csvlocation = localfilepath + '/' + vfilelist[1]
        vcolumnlist, dbcolumnlist = [],[]
        dbcolumnlist = postgresqlconn.get_table_columns(vfilelist[0][1], vfilelist[0][2])
        with open(csvlocation) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter = ',')
            for col in csv_reader:
                vcolumnlist.append(col)
                break
        if(collections.Counter(vcolumnlist[0])==collections.Counter(dbcolumnlist)):
            print("Both csv columns (", len(vcolumnlist[0]), ") and db columns (", len(dbcolumnlist), ") are same!")
            return True
        else:
            print("Both csv columns (", len(vcolumnlist[0]), ") and db columns (", len(dbcolumnlist), ") are NOT same!")
            print("CSV Column Headers are mismatching with DB CSV coloumn, please check!")
    except Exception as e:
        logger_report.error(e)
        raise e        

def initialize_connection_to_endpoint(customerid):
        print('Initializing the PostgreSQL DB Connection')
        secret_name = postgresqlconn.get_customer_endpoint(customerid)
        dotenv.set_key(dotenv_path,'REDSHIFTSQL_DATABASE', secret_name[1])
        dotenv.set_key(dotenv_path,'REDSHIFT_SECRET_NAME', secret_name[0])

def create_temp_table(vfilelist, localfilepath, s3uri):
    try:
        print('Building Temporary Table & Redshift Copy query')
        initialize_connection_to_endpoint(vfilelist[0][0])
        csvlocation = localfilepath + '/' + vfilelist[1]
        dbquerytemptable,tmptablename = build_temp_table_query(csvlocation, vfilelist)
        s3uri = s3uri + vfilelist[1]
        loadcsvquery = build_copy_redshift_query(csvlocation, s3uri, vfilelist)
        print('Creating Temporary Table and loading the details from CSV to Temporary table')
        redshiftconn.execute_query(dbquerytemptable)
        redshiftconn.execute_query(loadcsvquery)
        print('Temporary Table' + tmptablename + 'has been created and loaded the details sucessfully!')
        c = redshiftconn.compare_uid_count(tmptablename)
        if c:
            print('Compare UID Count Result', c)
        else:
            print('Comparision is failed, please re-check the CSV file')
        return True, tmptablename
    except Exception as e:
        logger_report.error(e)
        raise e

def fintrace_ingestion_process(tmptablename, vfilelist):
    print('Proceeding with Fintrace Ingestion process for the CSV file' + vfilelist[0][1])
    getcddfields = postgresqlconn.get_cdl_true_columns(vfilelist[0][1], vfilelist[0][2])
    pprid = postgresqlconn.get_dataprovider_prepurchase_id(vfilelist[0][1])
    dprid = redshiftconn.get_dataprovider_rid(vfilelist[0][1])
    ctrid = redshiftconn.get_contact_type_rid('personal')
    totaluflag, totaliflag = 0, 0
    for item in getcddfields:
        print('Proceeding with Fintrace Ingestion process for the contact type' + item[0])
        updateflag, insertflag = 0, 0
        getupdaterids = redshiftconn.check_for_existing_customer(tmptablename, item[1], item[0])
        hdrconv = item[0].lower()
        for eitem in getupdaterids:
            redshiftconn.update_control_table(eitem[hdrconv], eitem['rid'], item[1])
            redshiftconn.update_contact_data_data(eitem['recordid'], item[1], eitem[hdrconv], dprid[0], item[0])
            updateflag +=1
        totaluflag += updateflag
        print('There are' + updateflag + 'records have been updated!')
        getinsertrids = redshiftconn.check_for_new_customers(tmptablename, item[0])
        if len(getinsertrids) != 0:
            for nitem in getinsertrids:
                mrid = redshiftconn.insert_control_table(nitem[hdrconv], nitem['recordid'], item[1])
                iurid = redshiftconn.get_rid_identity_data(tmptablename, nitem['recordid'])
                for id in iurid:
                    redshiftconn.insert_contact_data_table(id['uid_rid'], ctrid[0], item[1], mrid[0], dprid[0], pprid[0], item[0])
                    redshiftconn.insert_data_table(id['uid_rid'], id['rid'], mrid[0], item[1], item[0])
                insertflag +=1
        totaliflag += insertflag
        print('There are' + insertflag + 'records have been inserted!')
    
             
if __name__ == "__main__":
    logger_report = LoggerReport()
    bucketName = os.getenv('BUCKET_NAME')
    remoteDirectoryName = os.getenv('BUCKET_DIRECTORY')
    postgresqlconn = Psycopg2Connection()
    redshiftconn = RedshiftdbConnection()
    localfilepath, s3uri = download_directory_from_s3(bucketName, remoteDirectoryName, logger_report)
    verified_files = csv_filename_check(localfilepath, logger_report)
    for index in verified_files:
        print(index[1])
        if index[0][2] == 'fintrace':
            csv_normalise_mobile_num(index, localfilepath, bucketName, remoteDirectoryName)
            compareresult = csv_column_check(index, localfilepath, logger_report)
            if compareresult:
                ttc_output, tmptablename = create_temp_table(index, localfilepath, s3uri)
                fintrace_ingestion_process(tmptablename, index)
            else:
                pass

