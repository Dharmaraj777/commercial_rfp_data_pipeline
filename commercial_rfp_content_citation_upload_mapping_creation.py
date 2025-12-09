import os
import requests
import pandas as pd
from .commercial_rfp_config_loader import ConfigLoader
import urllib.parse
from datetime import datetime
from .commercial_rfp_shared_logger import logger
import io
from .commercial_rfp_data_ingestion_utils import UtilityFunctions

class CitationMapper:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.utils = UtilityFunctions()
        self.cert_path = self.config_loader.cert_path
        self.thumbprint = self.config_loader.thumbprint
        self.client_id = self.config_loader.client_id
        self.tenant_id = self.config_loader.tenant_id
        self.folder_path = self.config_loader.output_folder_path
        self.site_url = self.config_loader.sharepoint_site_url
        self.output_container_name = self.config_loader.citation_map_conatiner
        self.blob_service_client = self.config_loader.blob_service_client
        self.container_client = self.config_loader.blob_service_client.get_container_client(self.config_loader.commercial_rfp_survey_content_doc_library)
    

    def upload_docx_files_to_SharePoint_and_create_citation_map(self):
        logger.info("Starting upload_citation_files_to_sharepoint process...")

        try:
            # --- Get Graph Token ---
            access_token = self.utils.get_graph_access_token(self.cert_path, self.thumbprint, self.client_id, self.tenant_id)


            site_id, drive_id, relative_folder_path = self.utils.resolve_sharepoint_site_and_drive_ids(
                    self.site_url, self.folder_path, access_token
                )
            
            # Upload each blob and collect mapping
            mapping = []
            for blob in self.container_client.list_blobs():
                blob_client = self.container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                item = self.utils.upload_file_to_sharepoint(site_id, drive_id, relative_folder_path, blob.name, blob_data, access_token)
                if item:
                    mapping.append({
                        "file_name": item["name"],
                        "preview_url": item["webUrl"]
                    })
                    # print(f"Uploaded: {item['name']}")
                    # print(f"URL: {item['webUrl']}")
                    logger.info(f"Uploaded: {item['name']} | URL: {item['webUrl']}")


            # Write mapping to Excel
            df = pd.DataFrame(mapping)
            # df.to_excel("rfp_content_docx_preview_mapping.xlsx", index=False)
            # print("Mapping written to rfp_content_docx_preview_mapping.xlsx")
            filename = "XXMapping written to rfp_content_docx_preview_mapping.xlsx"
            self.utils.upload_result_to_blob_container(
                    filename, df, self.output_container_name, self.blob_service_client
                )
        
            #Delete OLD documents
            today_str = datetime.utcnow().strftime('%Y-%m-%d')
            self.utils.delete_old_sharepoint_files(site_id, drive_id, relative_folder_path, access_token, today_str)

            logger.info("Citation mapping and SharePoint upload process completed successfully.")

        except Exception as e:
            logger.exception(f"Failed in upload_citation_files_to_sharepoint: {e}")


