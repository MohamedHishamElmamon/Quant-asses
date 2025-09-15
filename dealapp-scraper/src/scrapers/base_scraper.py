from abc import ABC, abstractmethod
from typing import List
from src.models import Property

class BaseScraper(ABC):
    """Base scraper class"""

    def __init__(self):
        self.properties: List[Property] = []
        self.seen_ids = set()

    @abstractmethod
    def scrape(self) -> int:
        """Main scraping method"""
        pass

    @abstractmethod
    def _save_results(self):
        """Save results method"""
        pass