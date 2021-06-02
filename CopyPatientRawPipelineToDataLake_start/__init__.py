# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging

import azure.functions as func
import azure.durable_functions as dfunc

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:

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
        client = dfunc.DurableOrchestrationClient(starter)
        instance_id = await client.start_new("CopyPatientRawPipelineToDataLake_orchestrator", None, patient_id)

        logging.info(f"Started orchestration with ID = '{instance_id}' to process PatientId = '{patient_id}'.")

        return client.create_check_status_response(req, instance_id)

    else:
        return func.HttpResponse(
                "Counldn't find required 'PatientId' input parameter to process request!",
                status_code=200
        )


    