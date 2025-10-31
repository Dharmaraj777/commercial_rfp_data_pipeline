
import json
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.storage.blob import BlobServiceClient

class ConfigLoader:
    _config_instance = None

    def __init__(self, config_file='config.json'):
        if not ConfigLoader._config_instance:
            # Load configuration from the config file only once
            with open(config_file, 'r') as f:
                self.config_details = json.load(f)
            
            self.connection_string = self.config_details['storage_connection_string']
            self.content_container_name = self.config_details['commercial_rfp_survey_content_library']
            self.prompt_container_name = self.config_details['commercial_rfp_survey_prompt_library']
            self.output_container_name = self.config_details['commercial_rfp_survey_ai_generated_output']
            self.commercial_rfp_logs = self.config_details['commercial_rfp_logs']
            self.citation_map_conatiner = self.config_details['commercial_rfp_survey_citation_map']
            self.files_status_container = self.config_details['commercial_rfp_survey_files_status']
            self.files_status_blob = self.config_details['commercial_rfp_files_processing_status']
            self.commercial_rfp_survey_raw_data_files = self.config_details['commercial_rfp_survey_raw_data_files']
            # Create a BlobServiceClient
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

            # Azure OpenAI Configuration
            self.openai_api_base = self.config_details['openai_api_base']
            self.openai_api_key = self.config_details['openai_api_key']
           
            self.embedding_model = self.config_details['openai_embedding_model']
    
            self.openai_api_version= self.config_details['large_model_api_version']
            self.deployment_id = self.config_details['large_model']
            
           
            # Azure Cognitive Search Configuration
            self.search_endpoint = self.config_details['cogsearch_endpoint']
            self.search_index_name = self.config_details['commercial_rfp_survey_content_doc_library_index']
            self.search_key = self.config_details['cogsearch_api_key']
            self.credential = AzureKeyCredential(self.search_key)
            self.search_client = SearchClient(self.search_endpoint, self.search_index_name, credential=self.credential)

            self.redis_host = self.config_details['azure_redis_host']
            self.redis_key = self.config_details['azure_redis_key']

            self.mapping_filename = self.config_details['commercial_rfp_mapping_filename']
            
            ConfigLoader._config_instance = self  # Cache the instance for later reuse

    @staticmethod
    def get_instance():
        if ConfigLoader._config_instance is None:
            ConfigLoader()  # Initialize if not already done
        return ConfigLoader._config_instance
