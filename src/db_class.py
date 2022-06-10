import sys
from dataclasses import dataclass
import json
import os
import boto3
import psycopg2
import datetime as dt
from dotenv import load_dotenv
import redshift_connector
from datetime import datetime

load_dotenv()  # take environment variables from .env.

# DB constants
POSTSQL_DATABASE = os.getenv('MYSQL_DATABASE')
POSTSQL_DB_SECRET_NAME = os.getenv('DB_SECRET_NAME')
REDSHIFT_DATABASE = os.getenv('REDSHIFTSQL_DATABASE')
REDSHIFT_DB_SECRET_NAME = os.getenv('REDSHIFT_SECRET_NAME')
DB_AWS_REGION = os.getenv("AWS_REGION")

class Psycopg2Connection():
    def __init__(self):
        try:
            self.session = boto3.session.Session()
            self.client = self.session.client(service_name = 'secretsmanager', region_name = DB_AWS_REGION)
            self.secret_name = POSTSQL_DB_SECRET_NAME
            self.database_name = POSTSQL_DATABASE
            self.get_secret_value_response = self.client.get_secret_value(SecretId=self.secret_name)
            if 'SecretString' in self.get_secret_value_response:
                self.secret = json.loads(self.get_secret_value_response['SecretString'])
                self.connection = psycopg2.connect(host=self.secret["host"],
                                            user=self.secret["username"],
                                            password=self.secret["password"],
                                            database=self.database_name)            
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(e)
            print("Exception occurred during DB connect, Unable to get secrets")
            sys.exit(1)

    def get_customer_endpoint(self, customer_id):
        query = '''SELECT secret_name, db_name
                    FROM public.customer_endpoint_control
                    WHERE customer_identifier LIKE %s
                    '''
        self.cursor.execute(query, (customer_id,))
        dbout = self.cursor.fetchall()
        dbresult = [item for t in dbout for item in t]
        return dbresult

    def get_table_columns(self, dataprovider, product):
        query = '''SELECT mc.csv
                FROM data_provider_ingestion_map_control mc
                JOIN data_provider_ingestion_control dp ON dp.rid = mc.dataprovider
                JOIN data_provider_product_ingestion_control dpp ON dpp.rid = mc.product
                WHERE dp.data_provider_name = %s
                AND dpp.product_name = %s
                AND mc.csv is NOT NULL
                '''
        self.cursor.execute(query, (dataprovider,product))
        dbout = self.cursor.fetchall()
        dbresult = [item for t in dbout for item in t]
        return dbresult
    
    def get_cdl_true_columns(self, dataprovider, product):
        query = '''SELECT mc.csv, mc.contact_data_type
                FROM data_provider_ingestion_map_control mc
                JOIN data_provider_ingestion_control dp ON dp.rid = mc.dataprovider
                JOIN data_provider_product_ingestion_control dpp ON dpp.rid = mc.product
                WHERE dp.data_provider_name = %s
                AND dpp.product_name = %s
                AND mc.contact_data_lookup=true
                AND mc.contact_data_type='email'
                '''
        self.cursor.execute(query, (dataprovider,product))
        dbout = self.cursor.fetchall()
        dbresult= set(dbout)
        return dbresult
    
    def get_idl_true_columns(self, dataprovider, product):
        query = '''SELECT mc.csv, mc.contact_data_type
                FROM data_provider_ingestion_map_control mc
                JOIN data_provider_ingestion_control dp ON dp.rid = mc.dataprovider
                JOIN data_provider_product_ingestion_control dpp ON dpp.rid = mc.product
                WHERE dp.data_provider_name = %s
                AND dpp.product_name = %s
                AND mc.contact_data_lookup=true
                '''
        self.cursor.execute(query, (dataprovider,product))
        dbout = self.cursor.fetchall()
        dbresult= set(dbout)
        return dbresult
    
    def get_dataprovider_prepurchase_id(self, dataprovider):
        query = '''SELECT pre_purchase from data_provider_ingestion_control WHERE data_provider_name = %s'''
        self.cursor.execute(query, (dataprovider,))
        dbout = self.cursor.fetchone()
        return dbout
    
class RedshiftdbConnection():
    def __init__(self):
        try:
            self.session = boto3.session.Session()
            self.client = self.session.client(service_name = 'secretsmanager', region_name = DB_AWS_REGION)
            self.secret_name = REDSHIFT_DB_SECRET_NAME
            self.database_name = REDSHIFT_DATABASE
            self.get_secret_value_response = self.client.get_secret_value(SecretId=self.secret_name)
            if 'SecretString' in self.get_secret_value_response:
                self.secret = json.loads(self.get_secret_value_response['SecretString'])
                self.connection = redshift_connector.connect(host=self.secret["host"],
                                            user=self.secret["username"],
                                            password=self.secret["password"],
                                            database=self.database_name)            
            self.rcursor = self.connection.cursor()
        except Exception as e:
            print(e)
            print("Exception occurred during DB connect, Unable to get secrets")
            sys.exit(1)
        finally:
            try:
                self.connection.commit()
            except psycopg2.InterfaceError:
                pass

    def execute_query(self, query: str):
        try:
            self.rcursor.execute(query)
            self.connection.commit()
            return True
        except Exception as e:
            print(e) 

    def compare_uid_count(self, tmptablename):
        query0 = ('''SELECT count(recordid) FROM {}''').format(tmptablename)
        query1 = ('''SELECT count(*) FROM {} tmp JOIN uid_control uc ON uc.uid = tmp.recordid''').format(tmptablename)
        self.rcursor.execute(query0)
        count0 = self.rcursor.fetchone()
        print('Number of recordid from CSV', count0)
        self.rcursor.execute(query1)
        count1 = self.rcursor.fetchone()
        print('Number of recordid from both CSV and uid_control', count1)
        if count0 == count1:
            return True
    
    def get_uid(self, tmptablename):
        query = ('''SELECT uc.uid
                FROM uid_control uc
                JOIN {} tt ON uc.uid = tt.recordid
                ''').format(tmptablename)
        self.rcursor.execute(query, (tmptablename,))
        dbout = self.cursor.fetchall()
        dbresult = [item for t in dbout for item in t]
        return dbresult
    
    def check_for_existing_customer(self, tmptablename, mode, contacttype):
        print(contacttype)
        query = ('''CREATE OR REPLACE VIEW [ExistingCustomersCountFor{}] AS
                SELECT uc.uid, tpft.{}, cdd.rid,tpft.recordid
                FROM contact_data_data cdd
                JOIN uid_control uc ON uc.rid = cdd.uid_rid
                JOIN contact_method_control cmc ON cmc.rid = cdd.contact_method_rid
                JOIN {}_control cv ON cv.rid = cdd.contact_data_rid
                JOIN {} tpft ON uc.uid = tpft.recordid
                WHERE uc.uid = tpft.recordid
                AND cmc.contact_method = '{}'
                AND REPLACE(cv.{},' ', '') = tpft.{}; 
                ''').format(contacttype, contacttype, mode, tmptablename, mode, mode, contacttype)
        query1 = ('''SELECT * FROM ExistingCustomersCountFor{}''').format(contacttype)
        self.rcursor.execute(query)
        self.rcursor.execute(query1)
        desc = self.rcursor.description
        column_names = [col[0].decode() for col in desc]
        data = [dict(zip(column_names, row))  
                for row in self.rcursor.fetchall()]
        return data
    
    def update_control_table(self, updatentry, rid, mode):
        query = ('''UPDATE copy_{}_control cec SET
                {} = %s
                WHERE cec.rid = %s ''').format(mode, mode)
        self.rcursor.execute(query, (updatentry, rid))
        self.connection.commit()
        return True
    
    def update_contact_data_data(self, recordid, mode, value, dprid, tmpmode):
        query=('''SELECT cdd.rid, client_populated, bydp
                FROM copy_contact_data_data cdd
                JOIN uid_control uc ON uc.rid = cdd.uid_rid
                JOIN contact_method_control cmc ON cmc.rid = cdd.contact_method_rid
                JOIN copy_{}_control cv ON cv.rid = cdd.contact_data_rid
                WHERE uc.uid = %s
                AND cmc.contact_method = %s
                AND REPLACE(cv.{},' ', '') = %s''').format(mode, mode)
        self.rcursor.execute(query, (recordid, mode, value))
        desc = self.rcursor.description
        column_names = [col[0].decode() for col in desc]
        data = [dict(zip(column_names, row))  
                for row in self.rcursor.fetchall()]
        for uitem in data:
            matched = False if uitem['client_populated'] else True
            additional = True if uitem['client_populated'] else False
            unique_flag = True if uitem['bydp'] == 0 else False
            dpx = 'dp'+ str(dprid)
            if uitem['bydp'] != None:
                byxdp = 'by'+ uitem['bydp'] +'dp'
                bydp = uitem['bydp'] + 1
            else:
                byxdp = 'by'+ str(dprid) +'dp'
                bydp = 0
            last_provided_date = datetime.now()
            query1 = (''' UPDATE copy_contact_data_data cdd SET matched = %s, additional = %s, unique_flag = %s, {} = True, bydp = %s, {} = True, last_provided_date = %s, mode = %s
                    WHERE contact_data_rid = %s''').format(dpx, byxdp)
            self.rcursor.execute(query1, (matched, additional, unique_flag, bydp, last_provided_date, tmpmode, uitem['rid']))
            self.connection.commit()
        return True
    
    def check_for_new_customers(self, tmptablename, contacttype):
        query = ('''SELECT tpft.recordid, tpft.{} 
                FROM {} tpft WHERE tpft.recordid NOT IN (SELECT uid FROM ExistingCustomersCountFor{})
                AND tpft.{} NOTNULL 
                ''').format(contacttype, tmptablename, contacttype, contacttype)
        self.rcursor.execute(query)
        desc = self.rcursor.description
        column_names = [col[0].decode() for col in desc]
        data = [dict(zip(column_names, row))  
                for row in self.rcursor.fetchall()]
        return data
    
    def get_rid_identity_data(self, tmptablename, uid):
        query = ('''SELECT id.rid, id.uid_rid FROM identity_data id
                JOIN {} tpft ON tpft.recordid = %s
                JOIN uid_control uc on tpft.recordid = uc.uid
                WHERE id.uid_rid = uc.rid
                AND id.title = tpft.title 
                AND id.first_name = tpft.forename 
                AND (CASE WHEN tpft.middlename isnull THEN id.middle_name isnull 
                     ELSE id.middle_name=tpft.middlename END)
                AND id.last_name = tpft.surname 
                AND (CASE WHEN tpft.dateofbirth isnull THEN id.dob isnull 
                     ELSE id.dob = tpft.dateofbirth END)
                ''').format(tmptablename)
        self.rcursor.execute(query, (uid,))
        desc = self.rcursor.description
        column_names = [col[0].decode() for col in desc]
        data = [dict(zip(column_names, row))  
                for row in self.rcursor.fetchall()]
        return data
    
    def insert_control_table(self, newentry, recid, mode):
        query = ('''INSERT INTO copy_{}_control ({}) VALUES (%s)''').format(mode,mode)
        self.rcursor.execute(query, (newentry,))
        query1 = (''' SELECT rid from copy_{}_control WHERE {} = %s ''').format(mode, mode)
        self.rcursor.execute(query1,(newentry,))
        dbout = self.rcursor.fetchone()
        self.connection.commit()
        return dbout

    def insert_data_table(self, uidrid, identityrid, moderid, mode, tmpmode):
        query = ('''INSERT INTO copy_{}_data (uid_rid, identity_rid, {}_rid, uploaded,mode) VALUES (%s, %s, %s, %s,%s)''').format(mode,mode)
        self.rcursor.execute(query, (uidrid, identityrid, moderid, datetime.now(),tmpmode))
        self.connection.commit()
        return True
    
    def get_dataprovider_rid(self, dataprovider):
        query = '''SELECT rid FROM data_provider_control dp WHERE dp.data_provider = %s'''
        self.rcursor.execute(query, (dataprovider,))
        dbout = self.rcursor.fetchone()
        return dbout
    
    def get_contact_type_rid(self, contacttype):
        query = '''SELECT rid FROM contact_type_control WHERE contact_type = %s '''
        self.rcursor.execute(query, (contacttype,))
        dbout = self.rcursor.fetchone()
        return dbout

    def insert_contact_data_table(self, uidrid, ctrid, cmrid, cvrid, dprid, pprid, tmpmode):
        query = ''' SELECT rid from copy_contact_data_data WHERE uid_rid = %s'''
        self.rcursor.execute(query, (uidrid,))
        ctdtrid = self.rcursor.fetchone()
        if len(ctdtrid) == 0:
            client_populated, unique_flag, additional = False, True, True
            matched, no_info, unmatched = False, False, False
            dpx = 'dp'+ str(dprid)
            dpdict = {'dp1': False, 'dp2': False, 'dp3': False, 'dp4': False, 'dp5': False, 'dp6': False, 'dp7': False, 'dp8': False, 'dp9': False, 'dp10': False}
            if dpx in dpdict.keys():
                dpdict.update({dpx : True})
            byxdp = 'by'+ str(dprid) +'dp'
            bydpdict = {'by1dp': False, 'by2dp': False, 'by3dp': False, 'by4dp': False, 'by5dp': False, 'by6dp': False, 'by7dp': False, 'by8dp': False, 'by9dp': False, 'by10dp': False}
            if byxdp in bydpdict.keys():
                bydpdict.update({byxdp : True})
            last_provided_date = datetime.now()
            query1 = '''INSERT INTO copy_contact_data_data (uid_rid, contact_type_rid, contact_method_rid, contact_data_rid, client_populated, unique_flag, additional,
                matched, no_info, unmatched, dp1, dp2, dp3, dp4, dp5, dp6, dp7, dp8, dp9, dp10, bydp, by1dp, by2dp, by3dp, by4dp, by5dp, by6dp, by7dp, by8dp, 
                by9dp, by10dp, first_dp, purchased, last_provided_date, mode) 
                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            '''
            self.rcursor.execute(query1, (uidrid, ctrid, cmrid, cvrid, client_populated, unique_flag, additional, matched, no_info, unmatched, dpdict['dp1'], 
                                dpdict['dp2'], dpdict['dp3'], dpdict['dp5'],dpdict['dp6'],dpdict['dp7'],dpdict['dp8'],dpdict['dp9'],1,bydpdict['by1dp'],bydpdict['by2dp'],bydpdict['by3dp'],bydpdict['by4dp'],bydpdict['by5dp'],
                                bydpdict['by6dp'],bydpdict['by7dp'],bydpdict['by8dp'],bydpdict['by9dp'],bydpdict['by10dp'],dprid,pprid,last_provided_date,tmpmode))
            self.connection.commit()
            return True