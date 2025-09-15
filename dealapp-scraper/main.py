#!/usr/bin/env python3
"""DealApp Scraper Main Entry Point"""

import logging
import sys
from datetime import datetime
from src.scrapers import DealAppScraper
from src.config import Settings

# Setup logging
log_filename = f"{Settings.LOG_DIR}/dealapp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main function"""
    try:
        logger.info("="*60)
        logger.info("DealApp Property Scraper")
        logger.info("="*60)

        scraper = DealAppScraper()
        count = scraper.scrape()

        logger.info(f"\nScraping completed successfully!")
        logger.info(f"Total properties scraped: {count}")

    except KeyboardInterrupt:
        logger.warning("\nScraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()