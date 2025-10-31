import os
import json
from datetime import datetime
import pandas as pd
from commercial_rfp_shared_logger import logger
from commercial_rfp_raw_data_ingestion_and_cleaning import DataIngestion
import time

def main():
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"commercial_rfp_data_processor_{today_date}.log"
    config_file='config.json'
    
    with open(config_file, 'r') as f:
        config_details = json.load(f)

    
    #Define all variable here. Remove this logic from the individual files
    data_ingestion = DataIngestion()
    doc_creation = ''
    logger.info("******************************************************")
    data_ingestion.commercial_rfp_data_cleaning()
    time.sleep(10)

    doc_creation#.commercial_rfp_data_cleaning()
    time.sleep(10)

if __name__ == "__main__":
    main()
