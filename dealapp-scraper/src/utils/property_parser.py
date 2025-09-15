from typing import Dict, Optional
import logging
from datetime import datetime
from src.models import Property

logger = logging.getLogger(__name__)

class PropertyParser:
    """Handles property data parsing"""

    @staticmethod
    def parse_property(ad: Dict) -> Optional[Property]:
        """Parse property data from API response"""
        try:
            property_type = ad.get('propertyType', {})
            property_type_ar = property_type.get('propertyType_ar', '')

            purpose = ad.get('purpose', '')
            if purpose == 'SALE':
                listing_type = 'sale'
                full_type = f"{property_type_ar} للبيع"
            elif purpose == 'RENT':
                listing_type = 'rent'
                full_type = f"{property_type_ar} للإيجار"
            else:
                listing_type = purpose.lower()
                full_type = property_type_ar

            district = ad.get('district', {})
            related_questions = ad.get('relatedQuestions', {})

            rooms_num = related_questions.get('roomsNumRange', '0')
            try:
                if '-' in str(rooms_num):
                    bedrooms = float(str(rooms_num).split('-')[0])
                else:
                    bedrooms = float(rooms_num) if rooms_num else 0
            except:
                bedrooms = 0

            location = ad.get('location', {}).get('value', {}).get('coordinates', [])
            lng = location[0] if len(location) > 0 else None
            lat = location[1] if len(location) > 1 else None

            return Property(
                type=full_type,
                listing_type=listing_type,
                city=ad.get('city', {}).get('name_ar', 'الرياض'),
                district=district.get('name_ar', ''),
                district_en=district.get('name_en', ''),
                price=f"{ad.get('price', 0):,}",
                price_numeric=float(ad.get('price', 0)),
                area=str(ad.get('area', '')),
                area_numeric=float(ad.get('area', 0)),
                bedrooms=bedrooms,
                ad_id=ad.get('_id', ''),
                code=ad.get('code', ''),
                title=ad.get('title', ''),
                lat=lat,
                lng=lng,
                created_at=ad.get('createdAt', ''),
                extraction_date=datetime.now()
            )
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None