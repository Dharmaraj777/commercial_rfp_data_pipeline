from io import BytesIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
from openpyxl import load_workbook
import io
import os
import snowflake.connector

class UtilityFunctions:

    def __init__(self):
        '''
        This function initalize static varibales set in config.json
        '''
     
    def upload_result_to_blob_container(self, file, df, output_container_name, blob_service_client ):
    '''
    This function uploads result to Azure Container
    1. Establish connection to Azure Blob Storage
    2. Create an in-memory bytes buffer
    3. Write Dataframe to the buffer as an Excel file
    
    '''
    """Write new_df as an Excel file directly to Azure Blob Storage without saving locally."""
    container_client = connect_to_blob_storage_output_container(output_container_name, blob_service_client)
    if container_client is None:
        logger.error(f"Failed to connect to Azure Blob Storage to upload results {output_container_name}.")
        return

    try:
        # Create an in-memory bytes buffer
        output_stream = io.BytesIO()

        # Write DataFrame to the buffer as an Excel file
        with pd.ExcelWriter(output_stream) as writer:
            df.to_excel(writer, index=False)

        # Move to the beginning of the stream
        output_stream.seek(0)

        # Upload the buffer to Azure Blob Storage
        container_client.upload_blob(name=file, data=output_stream, overwrite=True)

        logger.info(f"Excel file '{file}' uploaded successfully to Azure Blob Storage.")

    except Exception as e:
        logger.error(f"Error uploading Excel file '{file}' to Azure Blob Storage: Error: {e}")
