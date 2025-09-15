import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Application settings"""

    # API Configuration
    BASE_URL = os.getenv('DEALAPP_BASE_URL', 'https://api.dealapp.sa/production')
    API_ENDPOINT = '/ad'

    # Scraping Configuration
    TARGET_PROPERTIES = int(os.getenv('TARGET_PROPERTIES', 500))
    MAX_TOKENS = int(os.getenv('MAX_TOKENS', 20))
    REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', 0.5))
    TOKEN_DELAY = float(os.getenv('TOKEN_DELAY', 2))

    # Browser Configuration
    HEADLESS = os.getenv('HEADLESS', 'true').lower() == 'true'
    BROWSER_TIMEOUT = int(os.getenv('BROWSER_TIMEOUT', 30000))

    # Output Configuration
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'data')
    LOG_DIR = os.getenv('LOG_DIR', 'logs')

    # Ensure directories exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)