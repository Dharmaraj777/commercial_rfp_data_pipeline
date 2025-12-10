from datetime import datetime
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader
from .commercial_rfp_create_blob_containers import CreateContainers
from .commercial_rfp_indexer import AzureAISearchResourceManager
from .commercial_rfp_data_ingestion_utils import UtilityFunctions
from .commercial_rfp_raw_data_ingestion_and_cleaning import DataIngestion
from .commercial_rfp_content_doc_library_creation import DocLibraryCreator
from .commercial_rfp_delete_index_files_and_reset_indexer import IndexCleaner
from .commercial_rfp_content_citation_upload_mapping_creation import CitationMapper
import azure.functions as func

def commercial_rfp_data_pipeline(req: func.HttpRequest) -> func.HttpResponse:
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"commercial_rfp_data_processor_{today_date}.log"   
    config_loader = ConfigLoader.get_instance()
    utils = UtilityFunctions()
    logger.info("*********** Commercial RFP Data Pipeline: START ***********")

    try:
        container_creator = CreateContainers()
        container_creator.create_containers()

        indexer_creator = AzureAISearchResourceManager()
        indexer_creator.ensure_all_resources_exist()

        data_ingestion = DataIngestion()
        data_ingestion.commercial_rfp_data_cleaning()

        doc_creator = DocLibraryCreator()
        doc_creator.commerercial_rfp_content_doc_library_creation()

        index_cleaner = IndexCleaner()
        index_cleaner.commercial_rfp_delete_indexed_files_and_reset_indexer()

        citation_mapper = CitationMapper()
        citation_mapper.upload_docx_files_to_SharePoint_and_create_citation_map()

        logger.info("Commercial RFP data pipeline completed successfully.")
        utils.upload_log_to_blob(log_file_name, config_loader.commercial_rfp_logs, config_loader.blob_service_client)    
        return func.HttpResponse(f"Commercial RFP data pipeline completed successfully.", status_code=200)    
        
    except Exception as e:
        logger.error(f"An error occurred in the Commercial RFP data pipeline process: {e}")
        utils.upload_log_to_blob(log_file_name, config_loader.commercial_rfp_logs, config_loader.blob_service_client)
        return func.HttpResponse(f"An error occurred in the Commercial RFP data pipeline process: {e}", status_code=500)

   
