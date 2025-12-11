import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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

        # You can tune this or even move it to config later
        self.max_workers = getattr(self.config_loader, "sharepoint_max_workers", 5)

    def _list_sharepoint_items(self, site_id, drive_id, relative_folder_path, access_token):
        """List all items in the SharePoint folder with pagination."""
        headers = {"Authorization": f"Bearer {access_token}"}
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

        return items

    def _upload_single_blob_to_sharepoint(self, site_id, drive_id, relative_folder_path,
                                          access_token, blob_name):
        """
        Helper for concurrent uploads:
        - downloads blob bytes
        - uploads to SharePoint
        - returns the created item or None on failure
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()

            item = self.utils.upload_file_to_sharepoint(
                site_id,
                drive_id,
                relative_folder_path,
                blob_name,
                blob_data,
                access_token
            )
            if item:
                logger.info(f"[upload] NEW: {item['name']} | URL: {item['webUrl']}")
            return item
        except Exception as e:
            logger.exception(f"[upload] Failed for blob '{blob_name}': {e}")
            return None

    def upload_docx_files_to_SharePoint_and_create_citation_map(self):
        """
        - Upload only NEW .docx blobs (keys) to SharePoint (concurrently).
        - Delete SharePoint .docx files whose names (keys) are not in blob (concurrently).
        - Build mapping file ONLY from the current SharePoint files after cleanup.
        """
        logger.info("Starting upload_citation_files_to_sharepoint process...")

        try:
            # 1) Get Graph token and resolve site/drive/folder
            access_token = self.utils.get_graph_access_token(
                self.cert_path, self.thumbprint, self.client_id, self.tenant_id
            )

            site_id, drive_id, relative_folder_path = self.utils.resolve_sharepoint_site_and_drive_ids(
                self.site_url, self.folder_path, access_token
            )

            # 2) List existing items ONCE to know which names already exist (to avoid re-upload)
            items_before = self._list_sharepoint_items(
                site_id, drive_id, relative_folder_path, access_token
            )
            existing_names = {
                item.get("name")
                for item in items_before
                if item.get("name")
            }

            logger.info(f"Found {len(existing_names)} existing files in SharePoint citation folder.")

            # 3) Determine which blobs are NEW and should be uploaded
            new_blob_names = []
            for blob in self.container_client.list_blobs():
                blob_name = blob.name
                if not isinstance(blob_name, str) or not blob_name.lower().endswith(".docx"):
                    continue
                if blob_name in existing_names:
                    logger.info(f"[upload] Skipping existing SharePoint file: {blob_name}")
                    continue
                new_blob_names.append(blob_name)

            logger.info(f"[upload] {len(new_blob_names)} new .docx blobs to upload to SharePoint.")

            # 4) Upload NEW blobs concurrently
            if new_blob_names:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._upload_single_blob_to_sharepoint,
                            site_id,
                            drive_id,
                            relative_folder_path,
                            access_token,
                            blob_name
                        ): blob_name
                        for blob_name in new_blob_names
                    }

                    for future in as_completed(futures):
                        blob_name = futures[future]
                        try:
                            _ = future.result()
                        except Exception as e:
                            logger.exception(f"[upload] Unhandled error for '{blob_name}': {e}")

            # 5) Cleanup: delete SharePoint files whose names are NOT in blob (also concurrent)
            self.delete_sharepoint_files_not_in_blob(site_id, drive_id, relative_folder_path, access_token)

            # 6) Re-list items AFTER cleanup to build a clean mapping
            items_after = self._list_sharepoint_items(
                site_id, drive_id, relative_folder_path, access_token
            )
            logger.info(f"Found {len(items_after)} files in SharePoint citation folder after cleanup.")

            mapping_rows = []
            for item in items_after:
                name = item.get("name")
                url = item.get("webUrl")
                if not name or not name.lower().endswith(".docx"):
                    continue
                if url:
                    mapping_rows.append(
                        {
                            "file_name": name,
                            "preview_url": url,
                        }
                    )

            # 7) Build mapping DataFrame and enforce uniqueness of keys
            if mapping_rows:
                df = pd.DataFrame(mapping_rows)
            else:
                df = pd.DataFrame(columns=["file_name", "preview_url"])

            if not df.empty:
                dup_mask = df["file_name"].duplicated(keep=False)
                if dup_mask.any():
                    dup_df = df.loc[dup_mask].sort_values("file_name")
                    dup_names = dup_df["file_name"].unique().tolist()
                    logger.warning(
                        f"[MAPPING] Detected duplicate file_name keys in mapping: "
                        f"{len(dup_names)} duplicated keys. Examples: {dup_names[:10]}"
                    )
                df = df.drop_duplicates(subset=["file_name"], keep="last")

            mapping_blob_name = self.mapping_filename or "rfp_content_docx_preview_mapping.xlsx"

            self.utils.upload_result_to_blob_container(
                mapping_blob_name,
                df,
                self.output_container_name,
                self.blob_service_client
            )

            logger.info(
                f"Citation mapping written to blob '{mapping_blob_name}' "
                f"with {len(df)} unique keys (file_name)."
            )

            logger.info("Citation mapping and SharePoint upload process completed successfully.")

        except Exception as e:
            logger.exception(f"Failed in upload_docx_files_to_SharePoint_and_create_citation_map: {e}")

    def delete_sharepoint_files_not_in_blob(self, site_id, drive_id, relative_folder_path, access_token):
        """
        Delete SharePoint citation .docx files whose filenames (keys) do NOT exist
        in the blob container, using concurrent deletes.

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
            items = self._list_sharepoint_items(
                site_id, drive_id, relative_folder_path, access_token
            )
            logger.info(f"Found {len(items)} items in SharePoint citation folder (before delete).")

            # 3) Determine which SharePoint files to delete:
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

            if not to_delete:
                logger.info("No SharePoint files to delete; cleanup finished.")
                return

            # 4) Concurrent deletes
            def _delete_single(entry):
                file_id = entry["id"]
                file_name = entry["name"]
                if not file_id:
                    logger.warning(f"[delete] Skipping delete for {file_name}: missing id.")
                    return False

                delete_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}"
                del_resp = requests.delete(delete_url, headers=headers)

                if del_resp.status_code == 204:
                    logger.info(f"[delete] Deleted obsolete SharePoint file: {file_name}")
                    return True
                else:
                    logger.error(
                        f"[delete] Failed to delete {file_name}: "
                        f"{del_resp.status_code} - {del_resp.text}"
                    )
                    return False

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(_delete_single, entry): entry["name"]
                    for entry in to_delete
                }

                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        _ = future.result()
                    except Exception as e:
                        logger.exception(f"[delete] Unhandled error when deleting '{name}': {e}")

            logger.info("Cleanup completed: SharePoint files now aligned with blob keys.")

        except Exception as e:
            logger.exception(f"Failed in delete_sharepoint_files_not_in_blob: {e}")
