import pandas as pd
import re
from datetime import datetime
from io import BytesIO
import warnings
import urllib.parse
import requests
from commercial_rfp_shared_logger import logger
from commercial_rfp_config_loader import ConfigLoader
from commercial_rfp_data_ingestion_utils import UtilityFunctions

warnings.filterwarnings("ignore")

class DataIngestion:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.utils = UtilityFunctions()
        self.today_date = datetime.now().strftime("%Y-%m-%d")
        self.cert_path = self.config_loader.cert_path  
        self.thumbprint = self.config_loader.thumbprint  
        self.client_id = self.config_loader.client_id  
        self.tenant_id = self.config_loader.tenant_id  
        self.sharepoint_site_url = self.config_loader.sharepoint_site_url
        self.input_folder_path = self.config_loader.input_folder_path
        self.output_folder_path = self.config_loader.output_folder_path
        self.azure_connection_string = self.config_loader.connection_string
        self.azure_output_container_name = self.config_loader.content_container_name
        self.commercial_rfp_survey_raw_data_files = self.config_loader.commercial_rfp_survey_raw_data_files

    def download_latest_excel_from_sharepoint_folder(self, access_token, site_url, folder_path):
        headers = {"Authorization": f"Bearer {access_token}"}
        folder_path_plain = urllib.parse.unquote(folder_path).strip().strip("/")
        if not folder_path_plain:
            raise ValueError("folder_path cannot be empty.")
        parts = [p for p in folder_path_plain.split("/") if p]
        drive_name = parts[0]
        folder_rel = "/".join(parts[1:])
        parsed = urllib.parse.urlparse(site_url)
        hostname = parsed.hostname
        site_path = parsed.path.rstrip('/')
        site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}", headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
        drives_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)
        drives_resp.raise_for_status()
        drives = drives_resp.json().get("value", [])
        drive_id = None
        for d in drives:
            if d.get("name", "").strip().lower() == drive_name.strip().lower():
                drive_id = d["id"]
                break
        if not drive_id:
            raise ValueError(f"Drive (library) named '{drive_name}' not found.")
        if folder_rel:
            enc_folder = urllib.parse.quote(folder_rel.strip("/"), safe="/")
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc_folder}:/children"
        else:
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
        items_resp = requests.get(list_url, headers=headers)
        items_resp.raise_for_status()
        items = items_resp.json().get("value", [])
        excel_exts = (".xlsx", ".xls", ".xlsm")
        files = [it for it in items if "file" in it and it.get("name", "").lower().endswith(excel_exts)]
        if not files:
            where_ = f"{drive_name}/{folder_rel}" if folder_rel else drive_name
            raise FileNotFoundError(f"No Excel files found under '{where_}'.")
        def _parse_iso_z(dt_str: str) -> datetime:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        latest = max(files, key=lambda x: _parse_iso_z(x["lastModifiedDateTime"]))
        latest_rel_path = f"{folder_rel}/{latest['name']}" if folder_rel else latest["name"]
        enc_item_path = urllib.parse.quote(latest_rel_path.strip("/"), safe="/")
        download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc_item_path}:/content"
        dl_resp = requests.get(download_url, headers=headers)
        dl_resp.raise_for_status()
        return BytesIO(dl_resp.content), latest["name"]

    def clean_data(self, df):
        df.columns = df.columns.str.lower()
        df = df.applymap(lambda x: re.sub(r'\s+', ' ', str(x)).strip())
        # (other cleaning steps from your previous code...)
        return df

    def commercial_rfp_data_cleaning(self):
        logger.info('Processing commercial_rfp_data_cleaning request.')
        try:
            access_token = self.utils.get_graph_access_token(
                self.cert_path, self.thumbprint, self.client_id, self.tenant_id
            )
            input_stream, original_filename = self.download_latest_excel_from_sharepoint_folder(
                access_token, self.sharepoint_site_url, self.input_folder_path)
            df_rfp = pd.read_excel(input_stream, engine="openpyxl")
            df_rfp.columns = df_rfp.columns.str.lower()
            logger.info(f"Original dataset of 'RFP content': {df_rfp.shape}")
            # Upload raw to blob
            self.utils.upload_result_to_blob_container(
                original_filename, df_rfp, self.commercial_rfp_survey_raw_data_files, self.config_loader.blob_service_client
            )
            clean_df = self.clean_data(df_rfp)
            timestamp = datetime.now().strftime("%Y%m%d")
            rfp_filename = f"RFP_content_library_{timestamp}.xlsx"
            self.utils.upload_result_to_blob_container(
                rfp_filename, clean_df, self.azure_output_container_name, self.config_loader.blob_service_client
            )
            logger.info("Commercial RFP data cleaning completed successfully.")
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}", exc_info=True)
