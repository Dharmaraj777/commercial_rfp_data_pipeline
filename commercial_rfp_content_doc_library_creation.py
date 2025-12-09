from io import BytesIO
import pandas as pd
from docx import Document
from datetime import datetime
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader


class DocLibraryCreator:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.in_blob_client = self.config_loader.blob_service_client.get_container_client(
            self.config_loader.content_container_name)
        self.out_blob_client = self.config_loader.blob_service_client.get_container_client(
            self.config_loader.commercial_rfp_survey_content_doc_library)

    
    def read_excel_from_blob(self, blob_client, blob_name):
        stream = blob_client.get_blob_client(blob_name).download_blob().readall()
        return pd.read_excel(BytesIO(stream), engine="openpyxl")
    
    def get_latest_rfp_content_library_blob(self, blob_client):
        blobs = [b.name for b in blob_client.list_blobs()]
        # Only keep files matching the pattern
        files = []
        for name in blobs:
            if name.startswith("RFP_content_library_") and name.endswith(".xlsx"):
                try:
                    date_part = name.replace("RFP_content_library_", "").replace(".xlsx", "")
                    file_date = datetime.strptime(date_part, "%Y%m%d")
                    files.append((name, file_date))
                except Exception:
                    continue
        if not files:
            return None
        # Return the file with the latest date
        latest_file = max(files, key=lambda x: x[1])[0]
        return latest_file
    
    
    def create_docx_content(self, main_file_name, row, response_col):
        doc = Document()
        doc.add_paragraph(f"Source File Name: {main_file_name}")
    
        fields = [
            ("Client Name", "client name"),
            ("RFP Type", "rfp type"),
            ("Consultant", "consultant"),
            ("Date", "date"),
            ("Question", "question"),
            (response_col.title(), response_col),
            ("SME", "sme"),
        ]
    
        for label, key in fields:
            value = row.get(key, None)
            if pd.notna(value) and value != "" and key in row:
                doc.add_paragraph(f"{label}: {value}")
    
        buffer = BytesIO()
        doc.save(buffer)
        buffer.seek(0)
        return buffer.read()
    
    
    def commerercial_rfp_content_doc_library_creation(self):
        logger.info('Creating .doc files and uploaing to blob storage container.')
        try:
        
            latest_blob_name = self.get_latest_rfp_content_library_blob(self.in_blob_client)
            if not latest_blob_name:
                logger.error("No valid RFP_content_library_{timestamp}.xlsx files found in the blob container.")
                return
        
            # Deleting old blobs
            for blob in self.out_blob_client.list_blobs():
                self.out_blob_client.get_blob_client(blob.name).delete_blob()
    
            # --- Read Excel ---
            df = self.read_excel_from_blob(self.in_blob_client, latest_blob_name)
            response_col = None
            for col in ("response", "fixed answer"):
                if col in df.columns:
                    response_col = col
                    break

            reference_col = df.columns[0]
            for idx, row in df.iterrows():
                ref_val = row.get(reference_col, None)
                if pd.isna(ref_val) or str(ref_val).strip() == "":
                    continue
                # Normalize float/Excel number
                if isinstance(ref_val, float) and ref_val.is_integer():
                    ref_val = int(ref_val)
                docx_file_name = f"RFP_Content_Library_{ref_val}.docx"
                # Create .docx content with correct source file name
                docx_bytes = self.create_docx_content(latest_blob_name, row, response_col)
                self.out_blob_client.get_blob_client(docx_file_name).upload_blob(docx_bytes, overwrite=True)
            logger.info("Docx creation and upload completed.")
        except Exception as e:
            logger.critical(f"Pipeline failed: {e}", exc_info=True)
    