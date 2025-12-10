from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexerClient
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader
import time


class IndexCleaner:   
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.cogsearch_endpoint = self.config_loader.cogsearch_endpoint
        self.cogsearch_api_key = self.config_loader.cogsearch_api_key
        self.index_name = self.config_loader.index_name
        self.indexer_name = self.config_loader.indexer_name

    def delete_all_documents_from_index(self, search_client):
        while True:
            results = list(search_client.search("*", select="chunk_id", top=1000))
            if not results:
                break

            batch = [{"@search.action": "delete", "chunk_id": doc["chunk_id"]} for doc in results]
            result = search_client.upload_documents(documents=batch)
            logger.info(f"Deleted batch with {len(batch)} documents, result: {result}")

    def reset_and_run_indexer(self, indexer_client, indexer_name):
        try:
            logger.info(f"Resetting indexer: {indexer_name}")
            indexer_client.reset_indexer(indexer_name)

            # Delay before re-running
            logger.info("Waiting 10 seconds before running indexer...")
            time.sleep(10)

            logger.info(f"Running indexer: {indexer_name}")
            indexer_client.run_indexer(indexer_name)

            logger.info(f"Indexer '{indexer_name}' has been reset and re-run successfully.")
        except Exception as e:
            logger.exception(f"Error while resetting and running indexer '{indexer_name}': {e}")


    def commercial_rfp_delete_indexed_files_and_reset_indexer(self):
        logger.info('Processing commercial_rfp_delete_indexed_files_and_reset_indexer request.')

        try:            
            credential = AzureKeyCredential(self.cogsearch_api_key)
            indexer_client = SearchIndexerClient(endpoint=self.cogsearch_endpoint, credential=credential)

            if self.index_name :
                logger.info(f"Resetting index: {self.index_name}")

                search_client = SearchClient(endpoint=self.cogsearch_endpoint,
                                            index_name=self.index_name,
                                            credential=credential)

                self.delete_all_documents_from_index(search_client)
                logger.info(f"All documents deleted from '{self.index_name}'.")
                
            # Reset and run indexers
            if self.indexer_name:
                logger.info(f"Resetting and running indexer: {self.indexer_name}")
                self.reset_and_run_indexer(indexer_client, self.indexer_name)
        except Exception as e:
            logger.exception("Error resetting Azure Search indexes.")
