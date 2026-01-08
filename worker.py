import os
import sys
import time
import django
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "portal.settings")

# Initialize Django
django.setup()

from utils.trading_manager import LiveTradingManager
from logzero import logger

def main():
    logger.info("Starting worker...")
    try:
        manager = LiveTradingManager()
        manager.start()
        
        # Keep the main thread alive to allow background threads to run
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in the worker: {e}")

if __name__ == "__main__":
    main()