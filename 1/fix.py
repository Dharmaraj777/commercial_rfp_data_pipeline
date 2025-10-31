import os
import json
from datetime import datetime
import pandas as pd
from survey_engine_logger import logger
from survey_engine_utils import UtilityFunctions
from surveys_engine_content_processor import ContentProcessor
from medicare_concierge_surveys_oai_client import OaiClient
from medicare_concierge_survey_output_manager import OutputManager

def main():
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"medicare_concierge_survey_{today_date}.log"
    config_file='config.json'
    
    with open(config_file, 'r') as f:
        config_details = json.load(f)
        output_container_name =  config_details['medicare_concierge_surveys_ai_generated_output']
        prompt_container_name = config_details['medicare_concierge_surveys_content_library']
        survey_logs = config_details['medicare_concierge_surveys_logs']

    logger.info("Starting Medicare Concierge Survey Execution Process.")
    try:
        oai_client = OaiClient()
        result_manager = OutputManager()
        utils_func = UtilityFunctions()
        utils_func.main_processing( oai_client, result_manager, output_container_name, prompt_container_name,'process_medicare_concierge_for_sentiment_and_themes_df')
        # Upload log after all files are processed
        utils_func.upload_log_to_blob(log_file_name, survey_logs)

    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}")
        print(f"An error occurred in the main process: {e}")
        utils_func.upload_log_to_blob(log_file_name, survey_logs)

    logger.info("************* END of Processing File ******************")

if __name__ == "__main__":
    main()
