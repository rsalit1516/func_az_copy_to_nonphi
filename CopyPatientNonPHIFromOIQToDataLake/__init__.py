# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt


import os
import sys
import json
import logging

import pyodbc
import pandas as pd
import azure.functions as func

from uuid import UUID

from azure.storage.filedatalake import DataLakeServiceClient

VIEWS_TO_QUERY  = {
        "PatientData": "PatientsNoPHI",
        "PatientCases": "PatientCasesNoPHI",
        "DatumCalibration": "MotionSenseDatumCalibrationNoPHI",
        "ExerciseExecution": "MotionSenseExerciseExecutionNoPHI",
        "ExercisePrescription": "MotionSenseExercisePrescriptionNoPHI",
        "SurveyResponses": "PatientSurveyResponsesNoPHI",
        "CaseEvents": "PatientCaseEventsNoPHI",
        "SurveyResponses":"PatientSurveyResponsesNoPHI",
        "ADL": "MotionSensePatientADLNoPHI",
        "KneeAngleDistribution": "MotionSenseKneeAngleDistributionNoPHI",
        }
DRIVER = '{ODBC Driver 17 for SQL Server}'


def is_valid_uuid(uuid_to_test, version=4):
    """
    Check if uuid_to_test is a valid UUID.
     Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}    
     Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.

    """
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


def main(PatientId: str) -> str:

    # get database connetion parameters from Azure function configuration
    OIQ_database = os.getenv("OIQ_DATABASE")
    OIQ_server = os.getenv("OIQ_SERVER")
    OIQ_username = os.getenv("OIQ_USERNAME")
    OIQ_password = os.getenv("OIQ_PASSWORD")

    NONPHI_DIR =  os.environ["OUTPUT_DIR"]

    # get data lake parameters
    # Get data lake connection parameters
    STORAGE_ACCOUNT_NAME = os.environ["STORAGE_ACCOUNT_NAME"]
    STORAGE_ACCOUNT_KEY = os.environ["STORAGE_ACCOUNT_KEY"]
    STORAGE_FILESYSTEM = os.environ["STORAGE_FILESYSTEM"]

    # if not is_valid_uuid(PatientId):
    #     return json.dumps({"body": f"'{PatientId}' is not a valid uuid"})


    logging.info(f"Processing date for PatientId '{PatientId}'")
    output_dict = {}
    with pyodbc.connect('DRIVER='+DRIVER+';SERVER='+OIQ_server+';PORT=1433;DATABASE='+OIQ_database+';UID='+OIQ_username+';PWD='+ OIQ_password) as conn:
        conn.setencoding(encoding='utf-16le')

        # check that database has target views to query
        for field,check_view in VIEWS_TO_QUERY.items():
            check_df = pd.read_sql(f"SELECT * FROM information_schema.tables WHERE TABLE_NAME='{check_view}';",conn)
            assert len(check_df.index) == 1

        for field, view in VIEWS_TO_QUERY.items():
            cur_sql = f"""
                DECLARE @TargetPatient AS UNIQUEIDENTIFIER='{PatientId}';
                SELECT * FROM [dbo].[{view}] INNER JOIN (SELECT @TargetPatient AS TargetPatientId) t1 ON t1.TargetPatientId = PatientId
                """
            cur_df = pd.read_sql(cur_sql,conn).drop(columns=["TargetPatientId"])

            if len(cur_df.index)>0:

                date_cols = [c for c in cur_df.columns if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(cur_df[c])]
                cur_df[date_cols] = cur_df[date_cols].apply(lambda x:  pd.to_datetime(x).dt.strftime('%Y%m%dT%H%M%S'),axis=0)

                assert cur_df.iloc[0]["PatientId"].upper() == PatientId.upper()
                if field == "PatientData":
                    output_dict = {**cur_df.to_dict('index')[0], **output_dict} 
                else:
                    output_dict[field] = cur_df.drop(columns=["PatientId"]).to_dict('records')


    if len(output_dict.keys()) > 0:
        # generate output json data for writing in data lake
        output_json = json.dumps(output_dict)

        # Connect to storage
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", STORAGE_ACCOUNT_NAME), credential=STORAGE_ACCOUNT_KEY)
        file_system_client = service_client.get_file_system_client(file_system=STORAGE_FILESYSTEM)
        directory_client = file_system_client.get_directory_client(f"{NONPHI_DIR}/{PatientId}")

        content_length = len(output_json)
        output_filename = f"{PatientId}_OIQ_data.json"
        logging.info(f"Writing file '{output_filename}' ({content_length})")
        file_client = directory_client.create_file(output_filename)
        file_client.upload_data(data=output_json, length=content_length, overwrite=True)
        response = file_client.flush_data(content_length)
        
        file_client.close()
        directory_client.close()
        file_system_client.close()

        return json.dumps({"body": f"wrote {output_filename}"})
        
    else:
        
        return json.dumps({"body": f"Couldn't find any data for patient '{PatientId}'"})
        
