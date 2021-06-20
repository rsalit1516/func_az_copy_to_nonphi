# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import os
import sys
import time
import json
import random
import logging

import pyodbc
import numpy as np
import pandas as pd
from pymongo import MongoClient

from azure.storage.filedatalake import DataLakeServiceClient

PORT = 10255
DRIVER = '{ODBC Driver 17 for SQL Server}'

TARGET_DTYPES = {'WornState': bool,
                 'KneeAngle':np.int64,
                 'SrcPitch':np.int64,
                 'SrcRoll':np.int64,
                 'FsnPitch':np.int64,
                 'FsnRoll':np.int64,
                 'WearableTimestamp':np.int64,
                 'AppTimestamp': np.int64,
                 'UtcOffSet': str}

JSON_OBJECTS_TO_REMOVE = ["_ApiProcessed","_FunctionProcessed", "UtcOffset"]

def main(PatientId: str) -> str:

    # get database connetion parameters from Azure function configuration
    OIQ_database = os.getenv("OIQ_DATABASE")
    OIQ_server = os.getenv("OIQ_SERVER")
    OIQ_username = os.getenv("OIQ_USERNAME")
    OIQ_password = os.getenv("OIQ_PASSWORD")

    # Get data lake connection parameters
    STORAGE_ACCOUNT_NAME = os.environ["STORAGE_ACCOUNT_NAME"]
    STORAGE_ACCOUNT_KEY = os.environ["STORAGE_ACCOUNT_KEY"]
    STORAGE_FILESYSTEM = os.environ["STORAGE_FILESYSTEM"]
    NONPHI_DIR =  os.environ["OUTPUT_DIR"]

    # Get MongoDb connection parameters
    DATABASE_NAME = os.getenv("MONGODB_DATABASE")
    COLLECTION = os.getenv("MONGODB_RAW_COLLECTION")
    USERNAME = os.getenv("MONGODB_USERNAME")
    PASSWORD = os.getenv("MONGODB_PASSWORD")
    HOST = os.getenv("MONGODB_HOST")

    
 
    # Connect to MongoDB
    args = "ssl=true&retrywrites=false&ssl_cert_reqs=CERT_NONE"
    connection_uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}?{args}"
    client = MongoClient(connection_uri)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION]

    # Connect to storage
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", STORAGE_ACCOUNT_NAME), credential=STORAGE_ACCOUNT_KEY)
    file_system_client = service_client.get_file_system_client(file_system=STORAGE_FILESYSTEM)
    directory_client = file_system_client.get_directory_client(f"{NONPHI_DIR}/{PatientId}")

    # get random time lag for current PatientId 
    with pyodbc.connect('DRIVER='+DRIVER+';SERVER='+OIQ_server+';PORT=1433;DATABASE='+OIQ_database+';UID='+OIQ_username+';PWD='+ OIQ_password) as conn:
        with conn.cursor() as cursor:
            cur_sql = f"""
                    DECLARE @TargetPatient AS UNIQUEIDENTIFIER='{PatientId}';
                    SELECT NonPHITimeLag FROM [dbo].[PatientNonPHITimeLag_view] INNER JOIN (SELECT @TargetPatient AS TargetPatientId) t1 ON t1.TargetPatientId = PatientId
                    """
            row = cursor.execute(cur_sql)
            row = cursor.fetchone()
            if row is not None:
                time_lag = pd.Timedelta(row[0],unit='day') 
            else:
                return json.dumps({"body": "Couldn't find time lag to make no PHI- Aborting!"})
    

    th = int((pd.Timestamp.now() - pd.to_datetime(0, unit='s')).total_seconds())
    found_counter, copied_counter = 0, 0
    start_time = time.time()
    collection_query = {"PatientId":PatientId}
    for doc in collection.find(collection_query):
        found_counter += 1
        output_filename = f"{COLLECTION}/{doc['_id']}.json"

        #if not force_copy & output_file_exists
        output_file_exists = directory_client.get_file_client(output_filename).exists()
        
        if not output_file_exists:
            
            samples_df = pd.DataFrame.from_dict(doc["DataSamples"])
            #samples_df = samples_df.applymap(lambda x: x if x.isnumeric() else np.ma.masked).astype(TARGET_DTYPES)
            samples_df = samples_df.astype(TARGET_DTYPES)
            assert time_lag != 0
            
            if any(samples_df['AppTimestamp'] > th):
                timestamp_units ='ms'
            else:
                timestamp_units ='s'

            doc["StartAppFakeTimeStamp"] = (pd.to_datetime(samples_df["AppTimestamp"].min(), unit=timestamp_units) + time_lag).strftime('%Y%m%dT%H%M%S')

            samples_df["AppTimestamp"] = samples_df["AppTimestamp"] - samples_df["AppTimestamp"].min()
            samples_df = samples_df.drop(columns=["UtcOffSet"])
            
            doc["DataSamples"] = samples_df.to_dict("records")

            # remove keys marked up for removal
            cleaned_doc = {k: v for k,v in doc.items() if k not in JSON_OBJECTS_TO_REMOVE} 
        
            output_json = json.dumps(cleaned_doc)
            content_length = len(output_json)
            
            logging.info(f"Writing file '{output_filename}' ({content_length})")
            file_client = directory_client.create_file(output_filename)
            file_client.upload_data(data=output_json, length=content_length, overwrite=True)
            response = file_client.flush_data(content_length)
            file_client.close()
            copied_counter += 1

    elapsed_time = time.time() - start_time

    directory_client.close()
    file_system_client.close()

    func_output = f"[{copied_counter}]/[{found_counter}] [copied]/[found] documents from PatientId = '{PatientId}' in {elapsed_time/60} minutes)"

    return json.dumps({"body": func_output})
