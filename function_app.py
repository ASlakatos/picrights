import azure.functions as func
import logging
import json
import io
import os
from azure.storage.blob import BlobServiceClient
import openpyxl

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="picrights_http", methods=["POST"])
def picrights_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processing Excel file.')

    req_body = req.get_json()
    filename = req_body.get('filename')

    # 1. Connect to Blob Storage
    # For local testing, replace os.environ[...] with your actual connection string string
    # For production, add "StorageConnectionString" to your Azure Function App's Environment Variables
    connection_string = os.environ.get("StorageConnectionString")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    container_input = "input-excel"
    container_output = "output-excel"
    
    # 2. Download the Excel file from the input container into memory
    blob_client_in = blob_service_client.get_blob_client(container=container_input, blob=filename)
    download_stream = blob_client_in.download_blob()
    file_stream = io.BytesIO(download_stream.readall())
    
    # 3. Modify the Excel file
    wb = openpyxl.load_workbook(file_stream)
    ws = wb.active
    ws['A1'] = "Hello World"
    
    # Save changes to a ew memory streamn
    output_stream = io.BytesIO()
    wb.save(output_stream)
    output_stream.seek(0)
    
    # 4. Upload the modified file to the output container
    blob_client_out = blob_service_client.get_blob_client(container=container_output, blob=filename)
    blob_client_out.upload_blob(output_stream, overwrite=True)
    
    return func.HttpResponse(
        json.dumps({"message": "Success", "filename": filename}), 
        mimetype="application/json"
    )