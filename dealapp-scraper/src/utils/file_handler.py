import csv
import json
import os
from datetime import datetime
from typing import List
import logging
from src.config import Settings
from src.models import Property

logger = logging.getLogger(__name__)

class FileHandler:
    """Handles file operations"""

    @staticmethod
    def save_to_csv(properties: List[Property], timestamp: datetime) -> str:
        """Save properties to CSV file"""
        if not properties:
            logger.warning("No properties to save")
            return None

        filename = os.path.join(
            Settings.OUTPUT_DIR,
            f"dealapp_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
        )

        fieldnames = [
            'type', 'listing_type', 'city', 'district', 'district_en',
            'price', 'price_numeric', 'area', 'area_numeric', 
            'bedrooms', 'ad_id', 'code', 'title', 'lat', 'lng',
            'created_at', 'source', 'extraction_date'
        ]

        with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows([p.to_dict() for p in properties])

        logger.info(f"Results saved to {filename}")
        return filename

    @staticmethod
    def save_to_json(properties: List[Property], timestamp: datetime) -> str:
        """Save properties to JSON file"""
        if not properties:
            logger.warning("No properties to save")
            return None

        filename = os.path.join(
            Settings.OUTPUT_DIR,
            f"dealapp_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        )

        data = {
            'metadata': {
                'timestamp': timestamp.isoformat(),
                'total_properties': len(properties),
                'source': 'DealApp Scraper'
            },
            'properties': [p.to_dict() for p in properties]
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Results saved to {filename}")
        return filename