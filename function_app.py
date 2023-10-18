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
from forecast_retriever import retrieve_forecast_result
from forecast_executor import execute_forecast
from forecast_trigger import execute_forecast_trigger
from neuralprophet import NeuralProphet

app = func.FunctionApp()

@app.function_name(name="ForecastHandler")
@app.queue_trigger(arg_name="msg", queue_name="forecasting-queue-items",
                connection="AzureWebJobsStorage")  # Queue trigger
def handle_event(msg: func.QueueMessage) -> None:
    try:
        req_body = json.loads(msg.get_body().decode('utf-8'))
        #req_body = req.get_json()
        job_id = req_body.get('job_id')
        input_csv_blob_name = req_body.get('input_csv_name')

        forecast_periods = req_body.get('forecast_periods', 365 * 3)
        historic_predictions = req_body.get('historic_predictions', True)
        epochs = req_body.get('epochs', None)
        freq = req_body.get('freq', 'D')
        job_id = req_body.get('job_id')
        # Connect to your Azure Blob Storage account
        connection_string = "AzureWebJobsStorage"
        blob_service_client = BlobServiceClient.from_connection_string(os.environ[connection_string])
        download_blob_client = blob_service_client.get_blob_client(container=os.environ['container_name'], blob=input_csv_blob_name)

        # Download the CSV file from Blob Storage
        csv_data = download_blob_client.download_blob()
        csv_text = csv_data.content_as_text(encoding='utf-8')
        df = pd.read_csv(io.StringIO(csv_text))
        df['ds'] = pd.to_datetime(df['ds'])
        execute_forecast(df, freq, forecast_periods, historic_predictions, epochs, blob_service_client, job_id)

    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        
        
@app.function_name(name="ForecastTrigger")
@app.route(route="triggerForecasting", methods=["POST"])
@app.queue_output(arg_name="msg", 
                queue_name="forecasting-queue-items", 
                connection="AzureWebJobsStorage")
def forecast_trigger(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    return execute_forecast_trigger(msg)


@app.function_name(name="ForecastRetriever")
@app.route(route="retrieveForecasting", methods=["GET"])
def forecast_retrieve(req: func.HttpRequest) -> func.HttpResponse:
    return retrieve_forecast_result(req)

