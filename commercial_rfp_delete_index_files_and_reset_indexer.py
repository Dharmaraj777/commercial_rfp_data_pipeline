# import logging
from commercial_rfp_shared_logger import logger, log_stream
# import azure.functions as func
import json
import os
from io import BytesIO
from datetime import datetime
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexerClient
from commercial_rfp_config_loader import ConfigLoader

def load_config():
    config_path = os.path.join('config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
    with open(config_path, 'r') as f:
        return json.load(f)

def delete_all_documents_from_index(search_client):
    while True:
        results = list(search_client.search("*", select="chunk_id", top=1000))
        if not results:
            break

        batch = [{"@search.action": "delete", "chunk_id": doc["chunk_id"]} for doc in results]
        result = search_client.upload_documents(documents=batch)
        logger.info(f"Deleted batch with {len(batch)} documents, result: {result}")

def reset_indexer(indexer_client, indexer_name):
    indexer_client.reset_indexer(indexer_name)
    logger.info(f"Indexer '{indexer_name}' reset successfully.")


def upload_log_to_blob(blob_name, config_loader):
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

def commercial_rfp_delete_indexed_files_and_reset_indexer():
    logger.info('Processing commercial_rfp_delete_indexed_files_and_reset_indexer request.')

    try:
        config = load_config()
        config_loader = ConfigLoader.get_instance()
        today_date = datetime.now().strftime("%Y-%m-%d")
        log_file_name = f"commercial_rfp_data_processor_logs_{today_date}.log"

        service_endpoint = config["cogsearch_endpoint"]
        api_key = config["cogsearch_api_key"]
        index = [config["commercial_rfp_survey_index_name"]]
        indexer = [config["commercial_rfp_survey_indexer_name"]]
        
        credential = AzureKeyCredential(api_key)
        indexer_client = SearchIndexerClient(endpoint=service_endpoint, credential=credential)

        for index_name in index:
            logger.info(f"Resetting index: {index_name}")

            search_client = SearchClient(endpoint=service_endpoint,
                                         index_name=index_name,
                                         credential=credential)

            delete_all_documents_from_index(search_client)
            logger.info(f"All documents deleted from '{index_name}'.")
            
         # Reset and run indexers
        for indexer_name in indexer:
            logger.info(f"Resetting and running indexer: {indexer_name}")
            reset_indexer(indexer_client, indexer_name)
        upload_log_to_blob(log_file_name, config_loader)
        # return func.HttpResponse("Commercial RFP index cleared and indexer reset successfully", status_code=200)

    except Exception as e:
        logger.exception("Error resetting Azure Search indexes.")
        upload_log_to_blob(log_file_name, config_loader)
        # return func.HttpResponse(f"Error: {str(e)}", status_code=500)

#calling
commercial_rfp_delete_indexed_files_and_reset_indexer()
