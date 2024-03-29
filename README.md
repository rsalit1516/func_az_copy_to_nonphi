# func_az_copy_to_nonphi

This repository contains a series of Python Azure functions to copy data from OrthologIQ and Enmovi raw data to data science data lake.
These are Azure Durable functions and therefore are made up of an orchestrator function and the actual action function. Additionally, each function has a starter function that allows it to be called via html triggering for testing and debugging. Data factory calls the orchestrator functions directly.

Function for copying date from raw collection to the datalalake:
>**CopyPatientRawPipelineToDataLake**<br>
**CopyPatientRawPipelineToDataLake_orchestrator**<br>
**CopyPatientRawPipelineToDataLake_start** <br>

Function for copying date from gait collection to the datalalake:
>**CopyPatientGaitPipelineToDataLake**<br>
**CopyPatientGaitPipelineToDataLake_orchestrator**<br>
**CopyPatientGaitPipelineToDataLake_start** <br>

Function for copying date from OrthologIQ database to the datalalake:
>**CopyPatientNonPHIFromOIQToDataLake**<br>
**CopyPatientNonPHIFromOIQToDataLake_orchestrator**<br>
**CopyPatientNonPHIFromOIQToDataLake_start** <br>

In addition the Function App where the functions are to run require these settings to be defined in its configuration:<br>
- **MONGODB_DATABASE** - Name of the MongoDB database from where **CopyPatientGaitPipelineToDataLake** and **CopyPatientRawPipelineToDataLake** will get data from (e.g. "dev-enmovi-cosmosdb") <br>
- **MONGODB_USERNAME**: username used by **CopyPatientGaitPipelineToDataLake** & **CopyPatientRawPipelineToDataLake** to access MongoDB database<br>
- **MONGODB_PASSWORD**: password (key) used by **CopyPatientGaitPipelineToDataLake** & **CopyPatientRawPipelineToDataLake** to access MongoDB database<br>
 **MONGODB_HOST**: host used by **CopyPatientGaitPipelineToDataLake** & **CopyPatientRawPipelineToDataLake** to access MongoDB database (e.g. "dev-enmovi-cosmosdb.mongo.cosmos.azure.com")<br>
- **MONGODB_GAIT_COLLECTION**: MongoDB collection containing data from the *gait* pipeline (e.g. "dev-gait-analysis-enmovi-cosmosdb-collection")<br>
- **MONGODB_RAW_COLLECTION**: MongoDB collection containing data from the *raw* pipeline (e.g. "dev-raw-enmovi-cosmosdb-collection")<br>
- **OIQ_DATABASE**: Name of the database from where **CopyPatientNonPHIFromOIQToDataLake** will get data from (e.g. "OIQ_Latest")<br>
- **OIQ_PASSWORD**: password used by **CopyPatientNonPHIFromOIQToDataLake** to access OrthologIQ database<br>
- **OIQ_SERVER**: server uri used by **CopyPatientNonPHIFromOIQToDataLake** to access OrthologIQ<br>
- **OIQ_USERNAME**: username used by **CopyPatientNonPHIFromOIQToDataLake** to access OrthologIQ<br>
- **OUTPUT_DIR**: data lake directory where data is going to be saved (e.g. PatientData) <br>
- **STORAGE_ACCOUNT_KEY**: access key to the data lake storage account where data is going to be saved<br>
- **STORAGE_ACCOUNT_NAME**: name of the data lake storage account where data is going to be saved (e.g. "datasciencelakedev")<br>
- **STORAGE_FILESYSTEM**: (e.g. "datasciencelakedev")<br>

The CopyPatientNonPHIFromOIQToDataLake function expects the OrthologIQ database to have the following views defined:<br>
 -*list of view names to be added here*- <br>
As of the time of writing they are defined here - https://gitlab.com/strykercorp/enmovi/data-science/oiq_to_datascience_lake_sql
