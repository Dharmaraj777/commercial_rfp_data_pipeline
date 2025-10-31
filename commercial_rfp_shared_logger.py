import logging
from io import StringIO
import sys

log_stream = StringIO()

logger = logging.getLogger("commercial_rfp_data_pipeline_logger")
logger.setLevel(logging.INFO)

# Avoid adding handlers multiple times
if not logger.handlers:
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(console_handler)