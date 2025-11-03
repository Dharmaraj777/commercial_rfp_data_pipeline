import os
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from msal import ConfidentialClientApplication
import json
from commercial_rfp_config_loader import ConfigLoader
import urllib.parse
from datetime import datetime
from commercial_rfp_shared_logger import logger, log_stream
import io

def load_config():
    config_path = os.path.join('config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    with open(config_path, 'r') as f:
        return json.load(f)


def connect_to_blob_storage_output_container(output_container_name, blob_service_client):
    """Connect to Azure Blob Storage and create the container if it doesn't exist."""
    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(output_container_name)

        # Check if the container exists
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"Container '{output_container_name}' created.")

        return container_client

    except Exception as e:
        logger.error(f"Error connecting to Azure Blob Storage: {e}")
        return None

def upload_result_to_blob_container(file, df, output_container_name, blob_service_client ):
    '''
    This function uploads result to Azure Container
    1. Establish connection to Azure Blob Storage
    2. Create an in-memory bytes buffer
    3. Write Dataframe to the buffer as an Excel file
    
    '''
    """Write new_df as an Excel file directly to Azure Blob Storage without saving locally."""
    container_client = connect_to_blob_storage_output_container(output_container_name, blob_service_client)
    if container_client is None:
        logger.error(f"Failed to connect to Azure Blob Storage to upload results {output_container_name}.")
        return

    try:
        # Create an in-memory bytes buffer
        output_stream = io.BytesIO()

        # Write DataFrame to the buffer as an Excel file
        with pd.ExcelWriter(output_stream) as writer:
            df.to_excel(writer, index=False)

        # Move to the beginning of the stream
        output_stream.seek(0)

        # Upload the buffer to Azure Blob Storage
        container_client.upload_blob(name=file, data=output_stream, overwrite=True)

        logger.info(f"Excel file '{file}' uploaded successfully to Azure Blob Storage.")

    except Exception as e:
        logger.error(f"Error uploading Excel file '{file}' to Azure Blob Storage: Error: {e}")


# --- MSAL Graph Auth ---
def get_graph_access_token(cert_path, thumbprint, client_id, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]
    with open(cert_path, "rb") as f:
        cert_bytes = f.read()
    app = ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential={
            "private_key": cert_bytes,
            "thumbprint": thumbprint,
        }
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        return result["access_token"]
    raise Exception(f"MSAL Auth failed: {result.get('error')} - {result.get('error_description')}")
 

def upload_cititation_files_to_SharePoint():
 
    config = load_config()
    # --- MSAL/Graph parameters ---
    cert_path = config["sharepoint_cert_path"]  
    thumbprint = config["sharepoint_cert_thumbprint"]
    client_id = config["sharepoint_client_id"]
    tenant_id = config["sharepoint_tenant_id"]
    folder_path = "AI Data Repository/TEST"# config["commercial_rfp_sharepoint_content_doc_library"]
    site_url = config["commercial_rfp_sharepoint_site_url"]  
    # Azure Blob Storage details
    blob_connection_string = config["storage_connection_string"]
    blob_container_name = config["commercial_rfp_survey_content_doc_library"]
    output_container_name = config["commercial_rfp_survey_citation_map"]
     # Connect to Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    container_client = blob_service_client.get_container_client(blob_container_name)


    # --- Get Graph Token ---
    access_token = get_graph_access_token(cert_path, thumbprint, client_id, tenant_id)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream"
    }

    parsed = urllib.parse.urlparse(site_url)
    hostname = parsed.hostname
    site_path = parsed.path.rstrip('/')
    folder_parts = urllib.parse.unquote(folder_path).split('/')
    drive_name = folder_parts[0]
    relative_folder_path = '/'.join(folder_parts[1:]) if len(folder_parts) > 1 else ""
    
    # Discover site_id
    site_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
        headers=headers
    )
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    # Discover drive_id (doc library)
    drives_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
        headers=headers
    )
    drives_resp.raise_for_status()
    drives = drives_resp.json()["value"]
    drive_id = None
    for d in drives:
        if d["name"].strip().lower() == drive_name.strip().lower():
            drive_id = d["id"]
            break
    if not drive_id:
        raise Exception(f"Could not find drive '{drive_name}' at site '{site_url}'.")


    # Upload each blob and collect mapping
    mapping = []

    for blob in container_client.list_blobs():
        blob_client = container_client.get_blob_client(blob.name)
        blob_data = blob_client.download_blob().readall()

        # Upload to SharePoint folder
        # Upload file
        graph_path = f"{relative_folder_path}/{blob.name}" if relative_folder_path else blob.name
        upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{graph_path}:/content"
        
        #upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_path}/{blob.name}:/content"
        
        upload_response = requests.put(upload_url, headers=headers, data=blob_data)

        if upload_response.status_code in [200, 201]:
            item = upload_response.json()
            mapping.append({
                "file_name": item["name"],
                "preview_url": item["webUrl"]
            })
            print(f"Uploaded: {item['name']}")
            print(f"URL: {item['webUrl']}")
        else:
            print(f"Failed to upload {blob.name}: {upload_response.status_code} - {upload_response.text}")

    # Write mapping to Excel
    df = pd.DataFrame(mapping)
    # df.to_excel("rfp_content_docx_preview_mapping.xlsx", index=False)
    # print("Mapping written to rfp_content_docx_preview_mapping.xlsx")
    filename = "XXMapping written to rfp_content_docx_preview_mapping.xlsx"
    upload_result_to_blob_container(filename, df, output_container_name, blob_service_client )
  
    #Delete OLD documents
    today_str = datetime.utcnow().strftime('%Y-%m-%d')

    # List all items in the target folder
    list_items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{relative_folder_path}:/children"
    list_resp = requests.get(list_items_url, headers=headers)
    list_resp.raise_for_status()
    items = list_resp.json().get("value", [])

    # Loop through items and delete those not created today
    for item in items:
        created_date = item.get("createdDateTime", "")
        file_id = item.get("id")
        file_name = item.get("name")

        # Compare date portion only
        if not created_date.startswith(today_str):
            delete_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
            delete_resp = requests.delete(delete_url, headers=headers)
            if delete_resp.status_code == 204:
                print(f"Deleted: {file_name}")
            else:
                print(f"Failed to delete {file_name}: {delete_resp.status_code} - {delete_resp.text}")


upload_cititation_files_to_SharePoint()