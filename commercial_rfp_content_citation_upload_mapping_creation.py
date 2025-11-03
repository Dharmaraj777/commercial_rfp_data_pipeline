import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from commercial_rfp_shared_logger import logger
from commercial_rfp_config_loader import ConfigLoader
from commercial_rfp_data_ingestion_utils import UtilityFunctions

class CitationMapper:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.utils = UtilityFunctions()
        self.config = self.config_loader.config_details

    def upload_citation_files_to_sharepoint(self):
        logger.info("Starting upload_citation_files_to_sharepoint process...")
        try:
            cert_path = self.config["sharepoint_cert_path"]
            thumbprint = self.config["sharepoint_cert_thumbprint"]
            client_id = self.config["sharepoint_client_id"]
            tenant_id = self.config["sharepoint_tenant_id"]
            folder_path = self.config["commercial_rfp_sharepoint_content_doc_library"]
            site_url = self.config["commercial_rfp_sharepoint_site_url"]
            blob_connection_string = self.config["storage_connection_string"]
            blob_container_name = self.config["commercial_rfp_survey_content_doc_library"]
            output_container_name = self.config["commercial_rfp_survey_citation_map"]

            blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
            container_client = blob_service_client.get_container_client(blob_container_name)

            access_token = self.utils.get_graph_access_token(cert_path, thumbprint, client_id, tenant_id)
            site_id, drive_id, relative_folder_path = self.utils.resolve_sharepoint_site_and_drive_ids(
                site_url, folder_path, access_token
            )

            mapping = []
            for blob in container_client.list_blobs():
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                item = self.utils.upload_file_to_sharepoint(
                    site_id, drive_id, relative_folder_path, blob.name, blob_data, access_token
                )
                if item:
                    mapping.append({
                        "file_name": item["name"],
                        "preview_url": item["webUrl"]
                    })
                    logger.info(f"Uploaded: {item['name']} | URL: {item['webUrl']}")

            df = pd.DataFrame(mapping)
            filename = f"rfp_content_docx_preview_mapping_{datetime.utcnow().strftime('%Y%m%d')}.xlsx"
            self.utils.upload_result_to_blob_container(
                filename, df, output_container_name, blob_service_client
            )

            today_str = datetime.utcnow().strftime('%Y-%m-%d')
            self.utils.delete_old_sharepoint_files(site_id, drive_id, relative_folder_path, access_token, today_str)

            logger.info("Citation mapping and SharePoint upload process completed successfully.")

        except Exception as e:
            logger.exception(f"Failed in upload_citation_files_to_sharepoint: {e}")

if __name__ == "__main__":
    CitationMapper().upload_citation_files_to_sharepoint()
