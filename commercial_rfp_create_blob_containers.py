from azure.core.exceptions import ResourceExistsError
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader



class CreatContainers():
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        self.blob_service_client = self.config_loader.blob_service_client 

    def creat_containers(self):
        # List of containers to create
        container_names = [
            "commercial-rfp-survey-content-library",
            "commercial-rfp-survey-prompt-library",
            "commercial-rfp-survey-content-doc-library",
            "commercial-rfp-survey-ai-generated-output",
            "commercial-rfp-survey-citation-map",
            "commercial-rfp-survey-files-status",
            "commercial-rfp-survey-raw-data-files",
            "commercial-rfp-logs"
        ]

        for name in container_names:
            try:
                self.blob_service_client.create_container(name)
                logger.info(f"Created container: {name}")
            except ResourceExistsError:
                logger.info(f"Container '{name}' already exists. Skipping.")
            except Exception as e:
                logger.critical(f"Error creating container '{name}': {e}")