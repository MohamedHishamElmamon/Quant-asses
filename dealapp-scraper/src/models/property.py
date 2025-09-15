from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Property:
    """Property data model"""
    type: str
    listing_type: str
    city: str
    district: str
    district_en: str
    price: str
    price_numeric: float
    area: str
    area_numeric: float
    bedrooms: float
    ad_id: str
    code: str
    title: str
    lat: Optional[float] = None
    lng: Optional[float] = None
    created_at: Optional[str] = None
    source: str = 'DealApp API'
    extraction_date: Optional[datetime] = None

    def to_dict(self):
        """Convert to dictionary"""
        return {
            'type': self.type,
            'listing_type': self.listing_type,
            'city': self.city,
            'district': self.district,
            'district_en': self.district_en,
            'price': self.price,
            'price_numeric': self.price_numeric,
            'area': self.area,
            'area_numeric': self.area_numeric,
            'bedrooms': self.bedrooms,
            'ad_id': self.ad_id,
            'code': self.code,
            'title': self.title,
            'lat': self.lat,
            'lng': self.lng,
            'created_at': self.created_at,
            'source': self.source,
            'extraction_date': self.extraction_date.isoformat() if self.extraction_date else None
        }