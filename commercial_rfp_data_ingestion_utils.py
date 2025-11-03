from io import BytesIO
import pandas as pd
import io
import requests
import urllib.parse
from commercial_rfp_shared_logger import logger, log_stream

class UtilityFunctions:
    def __init__(self):
        pass

    def upload_result_to_blob_container(self, file, df, output_container_name, blob_service_client):
        try:
            output_stream = io.BytesIO()
            with pd.ExcelWriter(output_stream) as writer:
                df.to_excel(writer, index=False)
            output_stream.seek(0)
            container_client = blob_service_client.get_container_client(output_container_name)
            container_client.upload_blob(name=file, data=output_stream, overwrite=True)
            logger.info(f"Excel file '{file}' uploaded to Azure Blob Storage '{output_container_name}'.")
        except Exception as e:
            logger.error(f"Failed to upload '{file}' to Blob container '{output_container_name}': {e}")

    def upload_log_to_blob(self, blob_name, container_name, blob_service_client):
        try:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            log_data = log_stream.getvalue().encode('utf-8')
            blob_client.upload_blob(BytesIO(log_data), overwrite=True)
            logger.info(f"Log file {blob_name} uploaded successfully to '{container_name}'.")
        except Exception as e:
            logger.error(f"Failed to upload log file {blob_name} to '{container_name}': {e}")

    def get_graph_access_token(self, cert_path, thumbprint, client_id, tenant_id):
        from msal import ConfidentialClientApplication
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

    def resolve_sharepoint_site_and_drive_ids(self, site_url, folder_path, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        parsed = urllib.parse.urlparse(site_url)
        hostname = parsed.hostname
        site_path = parsed.path.rstrip('/')
        # Site ID
        site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}", headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
        # Drive ID
        folder_parts = urllib.parse.unquote(folder_path).split('/')
        drive_name = folder_parts[0]
        drives_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)
        drives_resp.raise_for_status()
        drives = drives_resp.json()["value"]
        drive_id = None
        for d in drives:
            if d["name"].strip().lower() == drive_name.strip().lower():
                drive_id = d["id"]
                break
        if not drive_id:
            raise Exception(f"Could not find drive '{drive_name}' at site '{site_url}'.")
        # Folder path inside drive (after the drive name)
        relative_folder_path = '/'.join(folder_parts[1:]) if len(folder_parts) > 1 else ""
        return site_id, drive_id, relative_folder_path

    def upload_file_to_sharepoint(self, site_id, drive_id, relative_folder_path, blob_name, file_bytes, access_token):
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/octet-stream"
        }
        graph_path = f"{relative_folder_path}/{blob_name}" if relative_folder_path else blob_name
        upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{graph_path}:/content"
        upload_response = requests.put(upload_url, headers=headers, data=file_bytes)
        if upload_response.status_code in [200, 201]:
            return upload_response.json()
        else:
            logger.error(f"Failed to upload {blob_name}: {upload_response.status_code} - {upload_response.text}")
            return None

    def delete_old_sharepoint_files(self, site_id, drive_id, relative_folder_path, access_token, keep_date):
        headers = {"Authorization": f"Bearer {access_token}"}
        list_items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{relative_folder_path}:/children"
        list_resp = requests.get(list_items_url, headers=headers)
        list_resp.raise_for_status()
        items = list_resp.json().get("value", [])
        for item in items:
            created_date = item.get("createdDateTime", "")
            file_id = item.get("id")
            file_name = item.get("name")
            if not created_date.startswith(keep_date):
                delete_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
                delete_resp = requests.delete(delete_url, headers=headers)
                if delete_resp.status_code == 204:
                    logger.info(f"Deleted old SharePoint file: {file_name}")
                else:
                    logger.error(f"Failed to delete {file_name}: {delete_resp.status_code} - {delete_resp.text}")
