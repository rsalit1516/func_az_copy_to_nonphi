# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as dfunc

def orchestrator_function(context: dfunc.DurableOrchestrationContext):
    target_PatientId = context.get_input()

    # get non-PHI data for target patient as a json
    result = yield context.call_activity('CopyPatientGaitPipelineToDataLake',target_PatientId)

    return [result]

main = dfunc.Orchestrator.create(orchestrator_function)