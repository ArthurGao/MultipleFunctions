import json
import uuid

import azure.functions as func


def execute_forecast_trigger(msg: func.Out[str]) -> func.HttpResponse:
    job_id = str(uuid.uuid4())

    data = {
        "input_csv_name": "sample_historical_data.csv",
        "job_id": job_id,
        "forecast_periods": 30,
        "historic_predictions": True,
        "epochs": 100,
        "freq": "D"
    }

    # Store the JSON data in the msg output binding
    msg.set(json.dumps(data))
    result = {
        "job_id": job_id
    }
    return func.HttpResponse(json.dumps(result), status_code=200)
