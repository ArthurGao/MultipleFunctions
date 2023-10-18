import datetime
import io
import json
import logging
import os
import tempfile
import uuid

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
from forecast_executor import execute_forecast
from forecast_trigger import execute_forecast_trigger
from neuralprophet import NeuralProphet


def retrieve_forecast_result(req: func.HttpRequest) -> func.HttpResponse:
    job_id = req.params.get('job_id')
    if not job_id:
        return func.HttpResponse("Missing 'job_id' query parameter", status_code=400)

    # Set Azure Storage connection string
    connection_string = "AzureWebJobsStorage"
    blob_service_client = BlobServiceClient.from_connection_string(os.environ[connection_string])

    container_name = os.environ['container_name']
    blob_name = job_id+".json"

    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    if not blob_client.exists():
        return func.HttpResponse("Forecast result not found", status_code=404)

    # Download the content of the blob
    blob_data = blob_client.download_blob()
    content = blob_data.readall()

    # Convert the content to JSON
    try:
        json_content = json.loads(content)
    except ValueError:
        return func.HttpResponse("Invalid JSON content in the blob", status_code=500)

    return func.HttpResponse(json.dumps(json_content), mimetype="application/json")