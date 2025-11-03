
import json
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
            self.commercial_rfp_logs = self.config_details['commercial_rfp_logs']
            self.citation_map_conatiner = self.config_details['commercial_rfp_survey_citation_map']
            self.commercial_rfp_survey_raw_data_files = self.config_details['commercial_rfp_survey_raw_data_files']
            # Create a BlobServiceClient
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            self.cert_path = self.config_details["sharepoint_cert_path"]  
            self.thumbprint = self.config_details["sharepoint_cert_thumbprint"] 
            self.client_id = self.config_details["sharepoint_client_id"]
            self.tenant_id = self.config_details["sharepoint_tenant_id"]
            self.sharepoint_site_url = self.config_details['commercial_rfp_sharepoint_site_url']
        
            self.input_folder_path = self.config_details['commercial_rfp_sharepoint_content_file_url']

            self.output_folder_path = self.config_details['commercial_rfp_sharepoint_content_output_folder_url']
            # self.azure_output_container_name = self.config_details['commercial_rfp_survey_content_library']
            # self.commercial_rfp_survey_raw_data_files = self.config_details['commercial_rfp_survey_raw_data_files']

          

            self.mapping_filename = self.config_details['commercial_rfp_mapping_filename']
            
            ConfigLoader._config_instance = self  # Cache the instance for later reuse

    @staticmethod
    def get_instance():
        if ConfigLoader._config_instance is None:
            ConfigLoader()  # Initialize if not already done
        return ConfigLoader._config_instance
