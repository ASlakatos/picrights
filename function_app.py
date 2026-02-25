import azure.functions as func
import logging
import json
import io
import os
from azure.storage.blob import BlobServiceClient
import openpyxl
import pandas as pd

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="picrights_http", methods=["POST"])
def picrights_http(req: func.HttpRequest) -> func.HttpResponse:

    # POST body elemntése
    req_body = req.get_json()   
    filename = req_body.get('filename')

    # Kapcsolat a storeg-al
    connection_string = os.environ.get("StorageConnectionString")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    container_input = "input-excel"
    container_output = "output-excel"
    
    # Input excel letöltése store-ból
    blob_client_in = blob_service_client.get_blob_client(container=container_input, blob=filename)
    download_stream = blob_client_in.download_blob()
    file_stream = io.BytesIO(download_stream.readall())
    
    # Excel fájl feldolgozása
    df_cases = pd.read_excel(file_stream, sheet_name="Cases")
    df_images = pd.read_excel(file_stream, sheet_name="Images")
    df_contacts = pd.read_excel(file_stream, sheet_name="Contacts")
    # Szerepelhet ugyanaz az infringer többször a clients sheet-en, miért? törlöm csak az elsőt hagyom meg
    df_contacts = df_contacts.drop_duplicates(subset=['ID Infringer'], keep='first')

    # Képek száma ID Case-enként
    df_image_counts = (
        df_images.groupby('ID Case')
        .size()
        .reset_index(name='ImageCount')
    )
    df_image_counts['Singular/Plural'] = (df_image_counts['ImageCount'] > 1).astype(int)

    # Cases, contacts merge
    df_merged = pd.merge(df_cases, df_contacts, on='ID Infringer', how='left')

    # Group by ID case (image sheet), minden más oszlopban összegezzük vesszővel elválasztva
    def aggregate_rows(series):
        return [str(x) for x in series if pd.notna(x) and str(x).lower() != 'none']
    df_images_collapsed = df_images.groupby('ID Case').agg(aggregate_rows).reset_index()

    # Cases, images merge
    final_df = pd.merge(df_merged, df_images_collapsed, on='ID Case', how='left')

    # Singular/Plural oszlop hozzáadása a Merged-hez
    final_df = pd.merge(final_df, df_image_counts[['ID Case', 'Singular/Plural']], on='ID Case', how='left')

    # Ha nincs kép az ID Case-hez, legyen 0
    final_df['Singular/Plural'] = final_df['Singular/Plural'].fillna(0).astype(int)
        
    # JSON letrehozas
    final_df_clean = final_df.fillna("")

    client_data = final_df_clean.to_dict(orient='records')

    # Változások elmentése, output file kiírása
    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine='openpyxl') as writer:
        df_cases.to_excel(writer, index=False, sheet_name='Cases')
        df_contacts.to_excel(writer, index=False, sheet_name='Contacts')
        df_images.to_excel(writer, index=False, sheet_name='Images')
        final_df.to_excel(writer, index=False, sheet_name='Merged')
    output_stream.seek(0)
    
    blob_client_out = blob_service_client.get_blob_client(container=container_output, blob=filename)
    blob_client_out.upload_blob(output_stream, overwrite=True)
    
    response_payload = {
            "filename": filename,
            "message": "Success",
            "data": client_data
        }

    # Response
    return func.HttpResponse( 
        json.dumps(response_payload), 
        mimetype="application/json"
    )