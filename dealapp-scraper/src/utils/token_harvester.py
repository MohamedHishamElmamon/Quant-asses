from playwright.sync_api import sync_playwright, Browser
from typing import Optional
import logging
from src.config import Settings, constants

logger = logging.getLogger(__name__)

class TokenHarvester:
    """Handles token harvesting from DealApp"""

    @staticmethod
    def harvest_single_token() -> Optional[str]:
        """Harvest a single token reliably"""
        browser = None
        try:
            with sync_playwright() as p:
                # Launch with specific args for Docker
                browser = p.chromium.launch(
                    headless=Settings.HEADLESS,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',  # Important for Docker
                        '--disable-setuid-sandbox',
                        '--no-sandbox',  # Required in Docker
                        '--disable-gpu',
                        '--disable-web-security',
                        '--disable-features=IsolateOrigins,site-per-process'
                    ]
                )

                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=constants.USER_AGENT,
                    locale='ar-SA',
                    timezone_id='Asia/Riyadh'
                )

                # Set extra headers
                context.set_extra_http_headers({
                    'Accept-Language': 'ar,en;q=0.9'
                })

                page = context.new_page()
                captured_token = None

                # Intercept requests to capture token
                def capture_token(request):
                    auth_header = request.headers.get('authorization', '')
                    if auth_header.startswith('Bearer ') and 'api.dealapp.sa' in request.url:
                        nonlocal captured_token
                        captured_token = auth_header.replace('Bearer ', '')
                        logger.debug(f"Token captured from request to: {request.url}")

                page.on('request', capture_token)

                try:
                    # Navigate to site with retry
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            page.goto('https://dealapp.sa', 
                                    wait_until='networkidle', 
                                    timeout=Settings.BROWSER_TIMEOUT)
                            break
                        except Exception as e:
                            logger.warning(f"Navigation attempt {attempt + 1} failed: {e}")
                            if attempt == max_retries - 1:
                                raise

                    # Wait for page to stabilize
                    page.wait_for_timeout(3000)

                    # Try multiple methods to trigger API calls
                    # Method 1: Click on market link
                    try:
                        market = page.locator('text=السوق').first
                        if market.is_visible():
                            market.click()
                            page.wait_for_timeout(2000)
                            logger.debug("Clicked on السوق")
                    except:
                        logger.debug("Could not click market link")

                    # Method 2: Try direct navigation to properties
                    if not captured_token:
                        try:
                            page.goto('https://dealapp.sa/ar/السوق/الاعلانات', 
                                    wait_until='domcontentloaded',
                                    timeout=10000)
                            page.wait_for_timeout(2000)
                        except:
                            pass

                    if captured_token:
                        logger.info("Successfully captured token")
                    else:
                        logger.warning("Failed to capture token from requests")

                except Exception as e:
                    logger.error(f"Error during token harvesting: {e}")
                finally:
                    context.close()

                browser.close()
                return captured_token

        except Exception as e:
            logger.error(f"Critical error in token harvesting: {e}")
            if browser:
                browser.close()
            return None