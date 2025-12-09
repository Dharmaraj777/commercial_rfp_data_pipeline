import pandas as pd
import re
from datetime import datetime
from io import BytesIO
import warnings
import urllib.parse
import requests
import hashlib
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader
from .commercial_rfp_data_ingestion_utils import UtilityFunctions

warnings.filterwarnings("ignore")


class DataIngestion():
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

    @staticmethod
    def _key_from_hash(text: str, algo: str = "md5") -> str:
        """Build a stable RFP_Content_* hash from a text snippet."""
        if text is None:
            text = ""
        snippet = text[:120]
        data = snippet.encode("utf-8")

        if algo == "md5":
            hash_hex = hashlib.md5(data).hexdigest()
        elif algo == "sha1":
            hash_hex = hashlib.sha1(data).hexdigest()
        elif algo == "sha256":
            hash_hex = hashlib.sha256(data).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algo}")

        return f"RFP_Content_{hash_hex}"

    def _add_rfp_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add `key` and `key_hash` columns to the final cleaned RFP dataframe."""
        df = df.copy()

        # Normalize date to a consistent string form if present
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        def build_key(row):
            client = str(row.get("client name", "")).strip()
            rfp_type = str(row.get("rfp type", "")).strip()
            consultant = str(row.get("consultant", "")).strip()
            date = str(row.get("date", "")).strip()
            question = str(row.get("question", "")).strip()
            return f"{client}_{date}_{rfp_type}_{consultant}_{question}"

        df["key"] = df.apply(build_key, axis=1)
        df["key_hash"] = (
            df["key"]
            .str.replace(r"\s+", "", "", regex=True)
            .apply(lambda x: self._key_from_hash(x, algo="md5"))
        )
        return df

    def download_latest_excel_from_sharepoint_folder(self, access_token: str, site_url: str, folder_path: str) -> BytesIO:
        headers = {"Authorization": f"Bearer {access_token}"}
    
        # Accept URL-encoded or plain input; normalize to plain
        folder_path_plain = urllib.parse.unquote(folder_path).strip().strip("/")
        if not folder_path_plain:
            raise ValueError("folder_path cannot be empty. Example: 'AI Data Repository/1. Content Library'")
    
        # Split the first segment as the library (drive) name; rest is the folder inside that drive
        parts = [p for p in folder_path_plain.split("/") if p]
        drive_name = parts[0]
        folder_rel = "/".join(parts[1:])  # may be empty
    
        # Determine site-id
        parsed = urllib.parse.urlparse(site_url)
        hostname = parsed.hostname
        site_path = parsed.path.rstrip("/")
        site_info_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
        site_resp = requests.get(site_info_url, headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
    
        # Find the drive by name
        drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        drives_resp = requests.get(drives_url, headers=headers)
        drives_resp.raise_for_status()
        drives = drives_resp.json().get("value", [])
        drive_id = None
        for d in drives:
            if d.get("name", "").strip().lower() == drive_name.strip().lower():
                drive_id = d["id"]
                break
        if not drive_id:
            raise ValueError(f"Drive '{drive_name}' not found at site '{site_url}'")
    
        # Build the children URL depending on whether we have a folder path or directly listing the root of the drive
        if folder_rel:
            # Access files under the given folder, e.g. "AI Data Repository/1. Content Library"
            # => drive: "AI Data Repository", folder: "1. Content Library"
            encoded_folder_rel = urllib.parse.quote(folder_rel.replace("\\", "/"))
            children_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_folder_rel}:/children"
        else:
            # No folder path inside the drive, just list the root
            children_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    
        # List all items in the folder (may require paging if there are many files)
        items = []
        while children_url:
            resp = requests.get(children_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            children_url = data.get("@odata.nextLink")
    
        # Filter to only Excel files and pick the last-modified one
        excel_items = [
            item for item in items 
            if not item.get("folder")
               and item["name"].lower().endswith((".xlsx", ".xls"))
        ]
        if not excel_items:
            raise ValueError(f"No Excel files found in folder '{folder_path_plain}'")
    
        # Pick the item with the most recent lastModifiedDateTime
        latest_item = max(excel_items, key=lambda x: x.get("lastModifiedDateTime", ""))
        file_name = latest_item["name"]
        file_id = latest_item["id"]
    
        # Download that file content
        download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}/content"
        download_resp = requests.get(download_url, headers=headers)
        download_resp.raise_for_status()
        file_bytes = download_resp.content
    
        return BytesIO(file_bytes), file_name

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.strip().str.lower()
        # Filter out rows where 'response' is null or empty
        if 'response' in df.columns:
            df = df[~df['response'].isna()]
            df = df[df['response'].astype(str).str.strip() != '']
        elif 'fixed answer' in df.columns:
            df.rename(columns={'fixed answer': 'response'}, inplace=True)
            df = df[~df['response'].isna()]
            df = df[df['response'].astype(str).str.strip() != '']
        else:
            logger.error("No response column found.")
            return pd.DataFrame()

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Standardize text columns if they exist
        for col in ['client name', 'rfp type', 'consultant', 'question', 'response']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        logger.info(f"Data after initial cleaning: {df.shape}")
        return df

    def drop_duplicates_same_question_and_response(self, df_rfp: pd.DataFrame) -> pd.DataFrame:
        df = df_rfp.copy()
        df = df.drop_duplicates(subset=['client name', 'rfp type', 'consultant', 'question', 'response'])
        logger.info(f"Data after dropping full duplicates on client/rfp/consultant/question/response: {df.shape}")
        return df

    def drop_duplicate_for_same_date_question(self, df_rfp: pd.DataFrame) -> pd.DataFrame:
        df = df_rfp.copy()
        if 'date' in df.columns:
            df = df.sort_values(by='date')
            df = df.drop_duplicates(subset=['date', 'client name', 'rfp type', 'consultant', 'question'], keep='last')
        logger.info(f"Data after dropping duplicates for same date-question: {df.shape}")
        return df

    def drop_duplicates_for_similar_questions(self, df_rfp: pd.DataFrame) -> pd.DataFrame:
        df = df_rfp.copy()
        if 'question' not in df.columns:
            return df

        # Normalize questions for length-based filtering
        df['question_clean'] = df['question'].astype(str).str.strip()
        df['value_length'] = df['question_clean'].str.len()

        df = df.sort_values(by='value_length', ascending=False)
        df_unique = df.drop_duplicates(subset=['question_clean'])

        df_unique = df_unique.drop(columns=['question_clean'])
        logger.info(f"Data after removing similar questions using question length: {df_unique.shape}")
        return df_unique

    def commercial_rfp_data_cleaning(self):
        logger.info('Processing commercial_rfp_data_cleaning request.')
        try:
            # Authenticate with MSAL/Graph
            access_token = self.utils.get_graph_access_token(self.cert_path, self.thumbprint, self.client_id, self.tenant_id)
            # Download Excel from SharePoint (using Graph, NOT username/password)
            input_stream, original_filename = self.download_latest_excel_from_sharepoint_folder(access_token, self.sharepoint_site_url, self.input_folder_path)
            df_rfp = pd.read_excel(input_stream, engine="openpyxl")
            df_rfp.columns = df_rfp.columns.str.lower()
            logger.info(f"Original dataset of 'RFP content': {df_rfp.shape}")

            # Uploading raw file to raw-data container
            self.utils.upload_result_to_blob_container(
                original_filename,
                df_rfp,
                self.commercial_rfp_survey_raw_data_files,
                self.config_loader.blob_service_client
            )

            clean_df = self.clean_data(df_rfp)
            filtered_df = self.drop_duplicates_same_question_and_response(clean_df)

            logger.info(f"After drop_duplicates_same_question_and_response: {filtered_df.shape}")
            df_unique_date_question = self.drop_duplicate_for_same_date_question(filtered_df)
            logger.info(f"After drop_duplicate_for_same_date_question: {df_unique_date_question.shape}")
            df_unique_date_question = self.drop_duplicates_for_similar_questions(df_unique_date_question)
            df_unique_date_question = df_unique_date_question.drop(columns=['value_length'])

            final_rfp_df = df_unique_date_question
            final_rfp_df['response'] = final_rfp_df['response'].str.replace(
                r'(?i)(CONFIRMED|CONFIRMED\.|Confirmed via BlueInsights\.|Confirmed via mail\.|Confirmed\.|Yes\.\s*Confirmed\.)',
                'Confirmed',
                regex=True
            )

            # Add stable key + hash per row
            final_rfp_df = self._add_rfp_keys(final_rfp_df)

            timestamp = datetime.now().strftime("%Y%m%d")
            rfp_filename = f"RFP_content_library_{timestamp}.xlsx"
            self.utils.upload_result_to_blob_container(
                rfp_filename,
                final_rfp_df,
                self.azure_output_container_name,
                self.config_loader.blob_service_client
            )

            logger.info("Commercial RFP data cleaning completed successfully.")
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}", exc_info=True)
