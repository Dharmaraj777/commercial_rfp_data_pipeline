import os
from io import BytesIO
import json
from typing import List
import re
import json
from commercial_rfp_shared_logger import logger, log_stream
from commercial_rfp_config_loader import ConfigLoader

import pandas as pd
from azure.storage.blob import BlobServiceClient
from docx import Document
from datetime import datetime
 
 
def load_config():
    config_path = os.path.join('config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    with open(config_path, 'r') as f:
        return json.load(f)
   
 
def init_blob_client(conn_str, container):
    blob_client = BlobServiceClient.from_connection_string(conn_str)
    return blob_client.get_container_client(container)
 
def read_excel_from_blob(blob_client, blob_name):
    stream = blob_client.get_blob_client(blob_name).download_blob().readall()
    return pd.read_excel(BytesIO(stream), engine="openpyxl")
 
def get_latest_rfp_content_library_blob(blob_client):
    blobs = [b.name for b in blob_client.list_blobs()]
    # Only keep files matching the pattern
    files = []
    for name in blobs:
        if name.startswith("RFP_content_library_") and name.endswith(".xlsx"):
            try:
                date_part = name.replace("RFP_content_library_", "").replace(".xlsx", "")
                file_date = datetime.strptime(date_part, "%Y%m%d")
                files.append((name, file_date))
            except Exception:
                continue
    if not files:
        return None
    # Return the file with the latest date
    latest_file = max(files, key=lambda x: x[1])[0]
    return latest_file
 

def detect_response_column(df):
    for col in ("response", "fixed answer"):
        if col in df.columns:
            return col
    raise KeyError("Neither 'response' nor 'fixed answer' column found")
 
def create_docx_content(main_file_name, row, response_col):
    doc = Document()
    doc.add_paragraph(f"Source File Name: {main_file_name}")
 
    fields = [
        ("Client Name", "client name"),
        ("RFP Type", "rfp type"),
        ("Consultant", "consultant"),
        ("Date", "date"),
        ("Question", "question"),
        (response_col.title(), response_col),
        ("SME", "sme"),
    ]
 
    for label, key in fields:
        value = row.get(key, None)
        if pd.notna(value) and value != "" and key in row:
            doc.add_paragraph(f"{label}: {value}")
 
    buffer = BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer.read()
 
def delete_all_blobs_in_container(blob_container):
    blob_list = blob_container.list_blobs()
    for blob in blob_list:
        blob_client = blob_container.get_blob_client(blob.name)
        blob_client.delete_blob()
 
 
 
def upload_to_blob(blob_container, file_name, content):
    try:
        blob_container.get_blob_client(file_name).upload_blob(content, overwrite=True)
    except Exception as e:
        logger.error(f"Failed to upload {file_name} to Azure Blob: {e}")
        raise
  
def upload_log_to_blob(blob_name, config_loader):
    try:
        blob_client = config_loader.blob_service_client.get_blob_client(
            container=config_loader.commercial_rfp_logs,
            blob=blob_name
        )
        log_data = log_stream.getvalue().encode('utf-8')
        blob_client.upload_blob(BytesIO(log_data), overwrite=True)
        logger.info(f"Log file {blob_name} uploaded successfully.")
    except Exception as e:
        logger.error(f"Failed to upload log file {blob_name}: {str(e)}")
 
def commerercial_rfp_content_doc_library_creation():
    logger.info('Creating .doc files and uploaing to blob storage container.')
    try:
     
        config = load_config()
   
        # Azure Blob clients
        in_blob_client = init_blob_client(
            config["storage_connection_string"],
            config["commercial_rfp_survey_content_library"]
        )
 
        out_blob_client = init_blob_client(
            config["storage_connection_string"],
            config["commercial_rfp_survey_content_doc_library"]
        )
 
        latest_blob_name = get_latest_rfp_content_library_blob(in_blob_client)
        if not latest_blob_name:
            logger.error("No valid RFP_content_library_{timestamp}.xlsx files found in the blob container.")
    
 
        delete_all_blobs_in_container(out_blob_client)
 
        # --- Read Excel ---
        df = read_excel_from_blob(in_blob_client, latest_blob_name)
        response_col = None
        for col in ("response", "fixed answer"):
            if col in df.columns:
                response_col = col
                break
        reference_col = df.columns[0]
 
        preview_urls = []
        for idx, row in df.iterrows():
            ref_val = row.get(reference_col, None)
            if pd.isna(ref_val) or str(ref_val).strip() == "":
                continue
            # Normalize float/Excel number
            if isinstance(ref_val, float) and ref_val.is_integer():
                ref_val = int(ref_val)
            docx_file_name = f"RFP_Content_Library_{ref_val}.docx"
            # Create .docx content with correct source file name
            docx_bytes = create_docx_content(latest_blob_name, row, response_col)
       
 
            upload_to_blob(out_blob_client, docx_file_name, docx_bytes)
 
 
        # Generate and Upload Mapping File to Blobsa
        config_loader = ConfigLoader.get_instance()
        today_date = datetime.now().strftime("%Y-%m-%d")
        log_file_name = f"commercial_rfp_data_processor_logs_{today_date}.log"
       
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
        upload_log_to_blob(log_file_name, config_loader)
 