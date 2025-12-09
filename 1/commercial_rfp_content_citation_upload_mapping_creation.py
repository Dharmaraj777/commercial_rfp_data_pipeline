import requests
import pandas as pd
import urllib.parse
from datetime import datetime

from .commercial_rfp_config_loader import ConfigLoader
from .commercial_rfp_shared_logger import logger
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
        self.container_client = self.config_loader.blob_service_client.get_container_client(
            self.config_loader.commercial_rfp_survey_content_doc_library
        )
        self.mapping_filename = self.config_loader.mapping_filename

    def upload_docx_files_to_SharePoint_and_create_citation_map(self):
        """Upload only new docx blobs to SharePoint and write the full mapping file.

        Existing SharePoint files are preserved so old citation URLs continue to work.
        """
        logger.info("Starting upload_citation_files_to_sharepoint process...")

        try:
            # Get Graph token
            access_token = self.utils.get_graph_access_token(
                self.cert_path, self.thumbprint, self.client_id, self.tenant_id
            )

            # Resolve site / drive / folder
            site_id, drive_id, relative_folder_path = self.utils.resolve_sharepoint_site_and_drive_ids(
                self.site_url, self.folder_path, access_token
            )

            headers = {"Authorization": f"Bearer {access_token}"}

            # List existing items in the SharePoint citation folder (with pagination)
            items = []
            base_url = (
                f"https://graph.microsoft.com/v1.0/sites/{site_id}"
                f"/drives/{drive_id}/root:/{relative_folder_path}:/children"
            )
            next_url = base_url

            while next_url:
                resp = requests.get(next_url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                items.extend(data.get("value", []))
                next_url = data.get("@odata.nextLink")

            existing_names = {item.get("name") for item in items if item.get("name")}
            mapping_rows = []

            # 1) Start mapping with ALL existing files in the SharePoint folder
            for item in items:
                name = item.get("name")
                url = item.get("webUrl")
                if name and url:
                    mapping_rows.append(
                        {
                            "file_name": name,
                            "preview_url": url,
                        }
                    )

            # 2) Upload only NEW blobs whose names are not already present in SharePoint
            for blob in self.container_client.list_blobs():
                blob_name = blob.name
                if not isinstance(blob_name, str) or not blob_name.lower().endswith(".docx"):
                    continue

                if blob_name in existing_names:
                    logger.info(f"Skipping existing SharePoint file: {blob_name}")
                    continue

                blob_client = self.container_client.get_blob_client(blob_name)
                blob_data = blob_client.download_blob().readall()

                item = self.utils.upload_file_to_sharepoint(
                    site_id, drive_id, relative_folder_path, blob_name, blob_data, access_token
                )
                if item:
                    mapping_rows.append(
                        {
                            "file_name": item["name"],
                            "preview_url": item["webUrl"],
                        }
                    )
                    logger.info(f"Uploaded NEW: {item['name']} | URL: {item['webUrl']}")

            # 3) Build mapping DataFrame (dedupe by file_name)
            if mapping_rows:
                df = pd.DataFrame(mapping_rows).drop_duplicates(subset=["file_name"])
            else:
                df = pd.DataFrame(columns=["file_name", "preview_url"])

            mapping_blob_name = self.mapping_filename or "rfp_content_docx_preview_mapping.xlsx"

            self.utils.upload_result_to_blob_container(
                mapping_blob_name, df, self.output_container_name, self.blob_service_client
            )

            # 4) Cleanup: delete SharePoint files whose keys are no longer in blob
            self.delete_sharepoint_files_not_in_blob(site_id, drive_id, relative_folder_path, access_token)

            logger.info("Citation mapping and SharePoint upload process completed successfully.")

        except Exception as e:
            logger.exception(f"Failed in upload_citation_files_to_sharepoint: {e}")

    def delete_sharepoint_files_not_in_blob(self, site_id, drive_id, relative_folder_path, access_token):
        """Delete SharePoint citation .docx files whose names do NOT exist in the blob container.

        - Keys are the filenames (e.g., RFP_Content_<hash>.docx).
        - Only .docx files are considered to avoid touching any non-docx artifacts.
        """
        logger.info("Starting cleanup: delete SharePoint files not present in Blob...")

        try:
            # 1) Build set of blob docx names (source of truth)
            blob_docx_names = {
                blob.name
                for blob in self.container_client.list_blobs()
                if isinstance(blob.name, str) and blob.name.lower().endswith(".docx")
            }
            logger.info(f"Found {len(blob_docx_names)} .docx files in blob container.")

            headers = {"Authorization": f"Bearer {access_token}"}

            # 2) List ALL items in the SharePoint citation folder (with pagination)
            items = []
            base_url = (
                f"https://graph.microsoft.com/v1.0/sites/{site_id}"
                f"/drives/{drive_id}/root:/{relative_folder_path}:/children"
            )
            next_url = base_url

            while next_url:
                resp = requests.get(next_url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                items.extend(data.get("value", []))
                next_url = data.get("@odata.nextLink")

            logger.info(f"Found {len(items)} items in SharePoint citation folder.")

            # 3) Decide which SharePoint files to delete:
            #    those .docx names NOT present in blob_docx_names
            to_delete = []
            for item in items:
                name = item.get("name")
                if not name or not name.lower().endswith(".docx"):
                    continue
                if name not in blob_docx_names:
                    to_delete.append(
                        {
                            "id": item.get("id"),
                            "name": name,
                        }
                    )

            logger.info(f"{len(to_delete)} SharePoint .docx files to delete (no matching blob key).")

            # 4) Delete obsolete SharePoint files
            for entry in to_delete:
                file_id = entry["id"]
                file_name = entry["name"]
                if not file_id:
                    logger.warning(f"Skipping delete for {file_name}: missing id.")
                    continue

                delete_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
                del_resp = requests.delete(delete_url, headers=headers)

                if del_resp.status_code == 204:
                    logger.info(f"Deleted obsolete SharePoint file: {file_name}")
                else:
                    logger.error(
                        f"Failed to delete {file_name}: "
                        f"{del_resp.status_code} - {del_resp.text}"
                    )

            logger.info("Cleanup completed: SharePoint files now aligned with blob keys.")

        except Exception as e:
            logger.exception(f"Failed in delete_sharepoint_files_not_in_blob: {e}")
