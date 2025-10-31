# import azure.functions as func
import pandas as pd
import re
from openpyxl import load_workbook
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import json
import os
import warnings
# import logging
from commercial_rfp_shared_logger import logger, log_stream
from commercial_rfp_config_loader import ConfigLoader
from msal import ConfidentialClientApplication
import urllib.parse
import requests
import json

warnings.filterwarnings("ignore")


class DataIngestion():
    def load_config():
        config_path = os.path.join('config.json')
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
        with open(config_path, 'r') as config_file:
            return json.load(config_file)

    try:
        config = load_config()
        config_loader = ConfigLoader.get_instance()
        today_date = datetime.now().strftime("%Y-%m-%d")
        log_file_name = f"commercial_rfp_data_processor_logs_{today_date}.log"
        cert_path = config["sharepoint_cert_path"]  
        thumbprint = config["sharepoint_cert_thumbprint"] 
        client_id = config["sharepoint_client_id"]
        tenant_id = config["sharepoint_tenant_id"]
        sharepoint_site_url = config['commercial_rfp_sharepoint_site_url']
    
        input_folder_path = config['commercial_rfp_sharepoint_content_file_url']

        output_folder_path = config['commercial_rfp_sharepoint_content_output_folder_url']
        azure_connection_string = config['storage_connection_string']
        azure_output_container_name = config['commercial_rfp_survey_content_library']
        commercial_rfp_survey_raw_data_files = config['commercial_rfp_survey_raw_data_files']
    except Exception as e:
        logger.critical(f"Config load failed: {e}")
        raise


    def get_graph_access_token(self, cert_path, thumbprint, client_id, tenant_id):
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


    def download_latest_excel_from_sharepoint_folder(self, access_token: str, site_url: str, folder_path: str) -> BytesIO:
        headers = {"Authorization": f"Bearer {access_token}"}
    
        # Accept URL-encoded or plain input; normalize to plain
        folder_path_plain = urllib.parse.unquote(folder_path).strip().strip("/")
        if not folder_path_plain:
            raise ValueError("folder_path cannot be empty. Example: 'AI Data Repository/1. Content Library'")
    
        # Split the first segment as the library (drive) name; rest is the folder inside that drive
        parts = [p for p in folder_path_plain.split("/") if p]
        drive_name = parts[0]
        folder_rel = "/".join(parts[1:])  # may be empty -> root of the library
    
        # Resolve site ID
        parsed = urllib.parse.urlparse(site_url)
        hostname = parsed.hostname
        site_path = parsed.path.rstrip('/')
        site_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}", headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
    
        # Get all drives (document libraries) on the site and find the correct one by display name
        drives_resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=headers)
        drives_resp.raise_for_status()
        drives = drives_resp.json().get("value", [])
    
        drive_id = None
        for d in drives:
            if d.get("name", "").strip().lower() == drive_name.strip().lower():
                drive_id = d["id"]
                break
    
        if not drive_id:
            available = ", ".join(sorted(d.get("name", "") for d in drives))
            raise ValueError(
                f"Drive (library) named '{drive_name}' not found. "
                f"Available libraries: {available or '[none]'}"
            )
    
        # List items in the target folder (or drive root if folder_rel empty) using colon-path syntax
        if folder_rel:
            enc_folder = urllib.parse.quote(folder_rel.strip("/"), safe="/")
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc_folder}:/children"
        else:
            list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
    
        items_resp = requests.get(list_url, headers=headers)
        items_resp.raise_for_status()
        items = items_resp.json().get("value", [])
    
        # Filter Excel files
        excel_exts = (".xlsx", ".xls", ".xlsm")
        files = [it for it in items if "file" in it and it.get("name", "").lower().endswith(excel_exts)]
        if not files:
            where_ = f"{drive_name}/{folder_rel}" if folder_rel else drive_name
            raise FileNotFoundError(f"No Excel files found under '{where_}'.")
    
        # Pick latest by lastModifiedDateTime
        def _parse_iso_z(dt_str: str) -> datetime:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    
        latest = max(files, key=lambda x: _parse_iso_z(x["lastModifiedDateTime"]))
        latest_rel_path = f"{folder_rel}/{latest['name']}" if folder_rel else latest["name"]
    
        # Download content via colon-path syntax
        enc_item_path = urllib.parse.quote(latest_rel_path.strip("/"), safe="/")
        download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc_item_path}:/content"
        dl_resp = requests.get(download_url, headers=headers)
        dl_resp.raise_for_status()
        print(latest["name"])
        return BytesIO(dl_resp.content), latest["name"]


    def upload_to_blob(self, connection_string, container_name, blob_name, df):
        try:
            output = BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(output.getvalue(), overwrite=True)
            logger.info(f"Uploaded to Azure Blob Storage: {blob_name}")
        except Exception as e:
            logger.error(f"Failed to upload to Blob: {blob_name}, error: {e}")
            raise


    def convert_list_to_dataframe(self, data):
        if data is not None and len(data) > 0:
            return pd.DataFrame(data[1:], columns=data[0])  # First row is header
        else:
            return pd.DataFrame()  # Empty DataFrame

    def get_length(self,value):
        if isinstance(value, str):
            return len(value)
        elif isinstance(value, (list, tuple, set)):
            return len(value)
        elif isinstance(value, (int, float)):
            return 1
        else:
            return None

    def parse_dates(self,date):
        if pd.isna(date):
            return pd.NaT
        for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y'):
            try:
                return pd.to_datetime(str(date), format=fmt)
            except Exception:
                continue
        return pd.to_datetime(date, errors='coerce')

    def clean_data(self, df):
        logger.info(f"Initial DataFrame shape: {df.shape}")

        df.columns = df.columns.str.lower()
        df = df.applymap(lambda x: re.sub(r'\s+', ' ', str(x)).strip())

        # logger.info(f"After trimming spaces, shape: {df.shape}")

        # --- Date column validation ---
        if 'date' not in df.columns:
            # Optionally, suggest close matches for user typo
            similar_cols = [col for col in df.columns if 'date' in col]
            if similar_cols:
                msg = f"'date' column not found. Did you mean: {similar_cols}?"
            else:
                msg = "'date' column not found in the data. Available columns: {}".format(list(df.columns))
            logger.critical(msg)
            raise KeyError(msg)

        # Now it's safe to operate on 'date' column
        df['date'] = df['date'].apply(parse_dates)
        logger.info(f"After parsing 'date', NaT count: {df['date'].isna().sum()}")

        df = df.dropna(subset=['date'])
        logger.info(f"After dropping NaT dates, shape: {df.shape}")

        # Use pandas DateOffset for months calculation (accurate)
        cutoff_date = (pd.Timestamp.now() - pd.DateOffset(months=36)).date()
        df['date'] = pd.to_datetime(df['date']).dt.date  # Ensure type safety
        df = df[df['date'] >= cutoff_date]
        logger.info(f"After filtering for last 36 months, shape: {df.shape}")

        if 'question' not in df.columns or 'response' not in df.columns:
            missing = []
            if 'question' not in df.columns:
                missing.append('question')
            if 'response' not in df.columns:
                missing.append('response')
            msg = f"Missing required columns: {missing}"
            logger.critical(msg)
            raise KeyError(msg)

        df = df[~df['question'].isna() & (df['question'].str.lower() != 'none')]
        logger.info(f"After removing rows where question is 'None', shape: {df.shape}")

        df = df[~df['response'].isna() & (df['response'].str.lower() != 'none')]
        logger.info(f"After removing rows where response is 'None', shape: {df.shape}")

        df = df[~df['response'].isna() & (df['response'].str.lower() != 'nan')]
        logger.info(f"After removing rows where response is 'Nan', shape: {df.shape}")

        df['value_length'] = df['response'].apply(get_length)
        df = df[df['value_length'] != 0]
        logger.info(f"After removing rows with empty response, shape: {df.shape}")

        df = df[~df['response'].str.lower().isin(['n/a', 'not applicable.'])]
        logger.info(f"After removing 'N/A' and 'Not applicable.', shape: {df.shape}")

        df = df[~df['question'].str.lower().isin(['contact'])]
        logger.info(f"After removing 'contact', shape: {df.shape}")

        df.reset_index(drop=True, inplace=True)
        return df


    def drop_duplicates_same_question_and_response(self,df):
        grouped_df = df.groupby(['question', 'response']).size().reset_index(name='count')
        duplicates = grouped_df[grouped_df['count'] > 1]
        logger.info(f"Total number of duplicates found (question & response are exact same): {duplicates['count'].sum()}")
        if not duplicates.empty:
            unique_duplicates_count = duplicates[['question', 'response']].drop_duplicates().shape[0]
            logger.info(f"Number of unique duplicate combinations: {unique_duplicates_count}")
            df_remove_dups = df.drop_duplicates(subset=['question', 'response'])
            logger.info(f"After removing duplicates (same question + same response): {df_remove_dups.shape}")
            return df_remove_dups
        else:
            return df

    def same_question_duplicate_response(self,df):
        if df is None:
            raise ValueError("The DataFrame is None. Please check the previous steps in your code.")
        duplicates = df[df.duplicated('question', keep=False)].sort_values(by=['question', 'date'])
        if not duplicates.empty:
            max_dates = duplicates.groupby('question')['date'].max().reset_index()
            df_kept = df[df['date'].isin(max_dates['date']) | ~df['question'].isin(duplicates['question'])]
        else:
            df_kept = df
        df_kept = df_kept.sort_values(by=['date', 'question'])
        logger.info(f"After removing duplicate questions with oldest dates: {df_kept.shape}")
        return df_kept

    def get_unique_date_question_with_longest_response(self, df):
        df_new = pd.DataFrame(df)
        df_new['char_count'] = df['response'].apply(len)
        df_sorted = df_new.loc[df_new.groupby(['question'])['char_count'].idxmax()]
        df_sorted = df_sorted.drop(columns=['char_count']).sort_values(by=['date', 'question'])
        logger.info(f"After removing duplicate questions with different responses by taking longest response: {df_sorted.shape}")
        return df_sorted

    def upload_log_to_blob(self, blob_name, config_loader):
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

    def commercial_rfp_data_cleaning(self):
        logger.info('Processing commercial_rfp_data_cleaning request.')
        try:
            # Authenticate with MSAL/Graph
            access_token = self.get_graph_access_token(cert_path, thumbprint, client_id, tenant_id)
            # Download Excel from SharePoint (using Graph, NOT username/password)
            input_stream, original_filename = self.download_latest_excel_from_sharepoint_folder(access_token, sharepoint_site_url, input_folder_path)
            df_rfp = pd.read_excel(input_stream, engine="openpyxl")
            df_rfp.columns = df_rfp.columns.str.lower()
            logger.info(f"Original dataset of 'RFP content': {df_rfp.shape}")

            #Uploading File to original container
            self.upload_to_blob(azure_connection_string, commercial_rfp_survey_raw_data_files, original_filename, df_rfp)

            clean_df = self.clean_data(df_rfp)
            filtered_df = self.drop_duplicates_same_question_and_response(clean_df)
            df_kept = self.same_question_duplicate_response(filtered_df)
            df_unique_date_question = self.get_unique_date_question_with_longest_response(df_kept)
            # Drop 'value_length' if present
            if 'value_length' in df_unique_date_question.columns:
                df_unique_date_question = df_unique_date_question.drop(columns=['value_length'])

            final_rfp_df = df_unique_date_question
            final_rfp_df['response'] = final_rfp_df['response'].str.replace(
                r'(?i)(CONFIRMED|CONFIRMED\.|Confirmed via BlueInsights\.|Confirmed via mail\.|Confirmed\.|Yes\.\s*Confirmed\.)',
                'Confirmed',
                regex=True
            )


            timestamp = datetime.now().strftime("%Y%m%d")
            rfp_filename = f"RFP_content_library_{timestamp}.xlsx"

            self.upload_to_blob(azure_connection_string, azure_output_container_name, rfp_filename, df_unique_date_question)

            logger.info("Commercial RFP data cleaning completed successfully.")
            self.upload_log_to_blob(log_file_name, config_loader)
            # return func.HttpResponse("Commercial RFP data cleaning completed successfully.", status_code=200)
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}", exc_info=True)
            self.upload_log_to_blob(log_file_name, config_loader)
            # return func.HttpResponse(f"Error: {str(e)}", status_code=500)


