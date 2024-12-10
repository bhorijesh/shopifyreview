import logging
import os

# Create a logs directory if it doesn't exist
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_directory}/scraping.log", mode='a'),
        logging.StreamHandler()
    ]
)

# Create a logger
logger = logging.getLogger("ScraperLogger")
