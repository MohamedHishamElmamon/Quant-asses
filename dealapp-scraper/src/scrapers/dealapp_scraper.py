import requests
import time
import logging
from datetime import datetime
from typing import Optional, List
from src.scrapers.base_scraper import BaseScraper
from src.utils import TokenHarvester, PropertyParser, FileHandler
from src.config import Settings, constants
from src.models import Property

logger = logging.getLogger(__name__)

class DealAppScraper(BaseScraper):
    """DealApp property scraper"""

    def __init__(self):
        super().__init__()
        self.timestamp = datetime.now()
        self.base_url = Settings.BASE_URL

    def scrape(self) -> int:
        """Main scraping method"""
        logger.info("Starting DealApp Scraper")

        token_count = 0

        while len(self.properties) < Settings.TARGET_PROPERTIES and token_count < Settings.MAX_TOKENS:
            token_count += 1
            logger.info(f"\n=== Session {token_count} ===")

            # Harvest a new token
            logger.info("Harvesting new token...")
            token = TokenHarvester.harvest_single_token()

            if token:
                logger.info(f"Token acquired: {token[:50]}...")

                # Use the token to scrape
                count = self._scrape_with_token(token, token_count)
                logger.info(f"Session {token_count} collected {count} new properties")
                logger.info(f"Total so far: {len(self.properties)} properties")

                # Small delay before next token
                time.sleep(Settings.TOKEN_DELAY)
            else:
                logger.error("Failed to harvest token, retrying...")
                time.sleep(5)

        logger.info(f"\nTotal unique properties collected: {len(self.properties)}")
        self._save_results()

        return len(self.properties)

    def _scrape_with_token(self, token: str, session_num: int) -> int:
        """Scrape using a specific token"""
        headers = constants.DEFAULT_HEADERS.copy()
        headers['Authorization'] = f'Bearer {token}'

        properties_count = 0

        for i, params in enumerate(constants.PARAM_COMBINATIONS):
            try:
                response = requests.get(
                    f"{self.base_url}{Settings.API_ENDPOINT}", 
                    params=params, 
                    headers=headers
                )

                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', [])

                    new_count = 0
                    for ad in ads:
                        if ad['_id'] not in self.seen_ids:
                            self.seen_ids.add(ad['_id'])
                            property_obj = PropertyParser.parse_property(ad)
                            if property_obj:
                                self.properties.append(property_obj)
                                properties_count += 1
                                new_count += 1

                    logger.info(f"Session {session_num}, Request {i+1}: Found {new_count} new properties")
                    time.sleep(Settings.REQUEST_DELAY)

                elif response.status_code == 403:
                    logger.warning(f"Session {session_num}: Token exhausted")
                    break

            except Exception as e:
                logger.error(f"Request error: {e}")

        return properties_count

    def _save_results(self):
        """Save results to files"""
        FileHandler.save_to_csv(self.properties, self.timestamp)
        FileHandler.save_to_json(self.properties, self.timestamp)

        # Log summary
        self._log_summary()

    def _log_summary(self):
        """Log scraping summary"""
        logger.info(f"\nSummary:")
        logger.info(f"- Total properties: {len(self.properties)}")

        if self.properties:
            districts = set(p.district for p in self.properties if p.district)
            logger.info(f"- Unique districts: {len(districts)}")

            prices = [p.price_numeric for p in self.properties if p.price_numeric > 0]
            if prices:
                logger.info(f"- Price range: {min(prices):,.0f} - {max(prices):,.0f} SAR")
                logger.info(f"- Average price: {sum(prices)/len(prices):,.0f} SAR")