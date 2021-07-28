import os
import json
import logging

import pyodbc
import pandas as pd

import azure.functions as func

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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f'Triggered request to processing non-PHI data')

    # get database connetion parameters from Azure function configuration
    OIQ_database = os.getenv("OIQ_DATABASE")
    OIQ_server = os.getenv("OIQ_SERVER")
    OIQ_username = os.getenv("OIQ_USERNAME")
    OIQ_password = os.getenv("OIQ_PASSWORD")

    # get function input parameters parameters
    patient_id = req.params.get('PatientId')
    if not patient_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            patient_id = req_body.get('PatientId')


    if patient_id:
        logging.info(f'Processing non-PHI data request for patient {patient_id}')

        output_dict = {}
        with pyodbc.connect('DRIVER='+DRIVER+';SERVER='+OIQ_server+';PORT=1433;DATABASE='+OIQ_database+';UID='+OIQ_username+';PWD='+ OIQ_password) as conn:
            conn.setencoding(encoding='utf-16le')

                # check that database has target views to query
            for field,check_view in VIEWS_TO_QUERY.items():
                check_df = pd.read_sql(f"SELECT * FROM information_schema.tables WHERE TABLE_NAME='{check_view}';",conn)
                assert len(check_df.index) == 1

            for field, view in VIEWS_TO_QUERY.items():
                cur_sql = f"""
                    DECLARE @TargetPatient AS UNIQUEIDENTIFIER='{patient_id}';
                    SELECT * FROM [dbo].[{view}] INNER JOIN (SELECT @TargetPatient AS TargetPatientId) t1 ON t1.TargetPatientId = PatientId
                    """
                cur_df = pd.read_sql(cur_sql,conn).drop(columns=["TargetPatientId"])
                date_cols = [c for c in cur_df.columns if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(cur_df[c])]
                cur_df[date_cols] = cur_df[date_cols].apply(lambda x:  pd.to_datetime(x).dt.strftime('%Y%m%dT%H%M%S'),axis=0)
                output_json = json.dumps(output_dict)

                if len(cur_df.index)>0:
                    assert cur_df.iloc[0]["PatientId"].upper() == patient_id.upper()
                    if field == "PatientData":
                        output_dict = {**cur_df.to_dict('index')[0], **output_dict} 
                    else:
                        output_dict[field] = cur_df.drop(columns=["PatientId"]).to_dict('records')
        
        output_json = json.dumps(output_dict)

        return func.HttpResponse(output_json,mimetype="application/json", status_code=200)
    else:
        return func.HttpResponse(
             "Counldn't find PatientId to process request!",
             status_code=200
        )
