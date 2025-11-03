import json
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from commercial_rfp_shared_logger import logger, log_stream

class UtilityFunctions:
    def __init__(self, config, today_date, log_file_name):
        self.config = config
        self.today_date = today_date
        self.log_file_name = log_file_name

    def upload_log_to_blob(self, log_file_name, logs_container):
        connection_string = self.config["storage_connection_string"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=logs_container, blob=log_file_name)
        log_data = log_stream.getvalue().encode('utf-8')
        blob_client.upload_blob(BytesIO(log_data), overwrite=True)
        logger.info(f"Log file {log_file_name} uploaded to {logs_container}")

    # Add all your shared/helper functions here (e.g., for blob, graph, etc.)

def load_config(config_file='config.json'):
    with open(config_file, 'r') as f:
        return json.load(f)
