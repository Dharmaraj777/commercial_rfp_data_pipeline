import pandas as pd
import re
from datetime import datetime
from io import BytesIO
import warnings
import urllib.parse
import requests
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader
from .commercial_rfp_data_ingestion_utils import UtilityFunctions
import hashlib

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
            .str.replace(r"\s+", "", regex=True)
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

    def parse_dates(self, date):
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
        df['date'] = df['date'].apply(self.parse_dates)
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

        df['value_length'] = df['response'].apply(self.get_length)
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

            #Uploading raw file to raw-data container
            self.utils.upload_result_to_blob_container( original_filename, df_rfp, self.commercial_rfp_survey_raw_data_files, self.config_loader.blob_service_client)


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

            # Add stable key + hash per row
            final_rfp_df = self._add_rfp_keys(final_rfp_df)


            timestamp = datetime.now().strftime("%Y%m%d")
            rfp_filename = f"RFP_content_library_{timestamp}.xlsx"

            self.utils.upload_result_to_blob_container(rfp_filename, final_rfp_df, self.azure_output_container_name, self.config_loader.blob_service_client)

            logger.info("Commercial RFP data cleaning completed successfully.")
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}", exc_info=True)


