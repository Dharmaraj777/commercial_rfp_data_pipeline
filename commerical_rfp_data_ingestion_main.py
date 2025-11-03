import json
from datetime import datetime
from commercial_rfp_shared_logger import logger
from commercial_rfp_data_ingestion_utils import UtilityFunctions
from commercial_rfp_raw_data_ingestion_and_cleaning import DataIngestion
from commercial_rfp_content_doc_library_creation import DocLibraryCreator
from commercial_rfp_delete_index_files_and_reset_indexer import IndexResetter

def main():
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"commercial_rfp_pipeline_{today_date}.log"
    config_file = 'config.json'

    with open(config_file, 'r') as f:
        config_details = json.load(f)
        storage_connection_string = config_details['storage_connection_string']
        output_container_name = config_details['commercial_rfp_survey_content_doc_library']
        input_container_name = config_details['commercial_rfp_survey_content_library']
        logs_container = config_details['commercial_rfp_logs']
        # Add any other config variables you need here

    logger.info("Starting Commercial RFP Data Pipeline.")
    try:
        utils_func = UtilityFunctions(config_details, today_date, log_file_name)

        # Step 1: Clean and publish Excel
        data_ingestion = DataIngestion(config_details)
        data_ingestion.commercial_rfp_data_cleaning()

        # Step 2: Generate DOCX library
        doc_creator = DocLibraryCreator(config_details)
        doc_creator.create_doc_library()

        # Step 3: (Optional) Reset index/files
        index_resetter = IndexResetter(config_details)
        index_resetter.reset_index_and_files()

        utils_func.upload_log_to_blob(log_file_name, logs_container)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
        UtilityFunctions(config_details, today_date, log_file_name).upload_log_to_blob(log_file_name, logs_container)

    logger.info("************* END of Processing File ******************")

if __name__ == "__main__":
    main()
