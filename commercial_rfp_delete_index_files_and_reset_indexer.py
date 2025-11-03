import json
from commercial_rfp_shared_logger import logger
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexerClient
from commercial_rfp_config_loader import ConfigLoader

class IndexCleaner:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        with open('config.json', 'r') as f:
            self.config = dict(**self.config_loader.config_details, **json.load(f))
        self.service_endpoint = self.config["cogsearch_endpoint"]
        self.api_key = self.config["cogsearch_api_key"]
        self.index_names = [self.config["commercial_rfp_survey_index_name"]]
        self.indexer_names = [self.config["commercial_rfp_survey_indexer_name"]]

    def delete_all_documents_from_index(self, search_client):
        while True:
            results = list(search_client.search("*", select="chunk_id", top=1000))
            if not results:
                break
            batch = [{"@search.action": "delete", "chunk_id": doc["chunk_id"]} for doc in results]
            search_client.upload_documents(documents=batch)
            logger.info(f"Deleted batch with {len(batch)} documents.")

    def reset_indexer(self, indexer_client, indexer_name):
        indexer_client.reset_indexer(indexer_name)
        logger.info(f"Indexer '{indexer_name}' reset successfully.")

    def commercial_rfp_delete_indexed_files_and_reset_indexer(self):
        logger.info('Processing delete indexed files and reset indexer...')
        try:
            credential = AzureKeyCredential(self.api_key)
            indexer_client = SearchIndexerClient(endpoint=self.service_endpoint, credential=credential)
            for index_name in self.index_names:
                logger.info(f"Resetting index: {index_name}")
                search_client = SearchClient(endpoint=self.service_endpoint,
                                             index_name=index_name,
                                             credential=credential)
                self.delete_all_documents_from_index(search_client)
            for indexer_name in self.indexer_names:
                self.reset_indexer(indexer_client, indexer_name)
            logger.info("All index files deleted and indexer reset successfully.")
        except Exception as e:
            logger.exception("Error resetting Azure Search indexes.")
