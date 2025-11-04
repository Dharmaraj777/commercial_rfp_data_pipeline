from datetime import datetime
from commercial_rfp_shared_logger import logger
from commercial_rfp_config_loader import ConfigLoader
from commercial_rfp_create_blob_containers import CreatContainers
from commercial_rfp_data_ingestion_utils import UtilityFunctions
from commercial_rfp_raw_data_ingestion_and_cleaning import DataIngestion
from commercial_rfp_content_doc_library_creation import DocLibraryCreator
from commercial_rfp_delete_index_files_and_reset_indexer import IndexCleaner
from commercial_rfp_content_citation_upload_mapping_creation import CitationMapper

def main():
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"commercial_rfp_data_processor_{today_date}.log"
    config_loader = ConfigLoader.get_instance()
    utils = UtilityFunctions()
    logger.info("*********** Commercial RFP Data Pipeline: START ***********")

    try:
        container_creator = CreatContainers()
        container_creator.creat_containers()

        data_ingestion = DataIngestion()
        data_ingestion.commercial_rfp_data_cleaning()

        doc_creator = DocLibraryCreator()
        doc_creator.commerercial_rfp_content_doc_library_creation()

        index_cleaner = IndexCleaner()
        index_cleaner.commercial_rfp_delete_indexed_files_and_reset_indexer()

        citation_mapper = CitationMapper()
        citation_mapper.upload_citation_files_to_sharepoint()

        utils.upload_log_to_blob(log_file_name, config_loader.commercial_rfp_logs, config_loader.blob_service_client)
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")
        utils.upload_log_to_blob(log_file_name, config_loader.commercial_rfp_logs, config_loader.blob_service_client)

    logger.info("*********** END of Processing File ***********")

if __name__ == "__main__":
    main()
