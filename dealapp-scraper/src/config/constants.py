"""Application constants"""

# User Agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# Headers
DEFAULT_HEADERS = {
    'Accept': 'application/json',
    'Accept-Language': 'ar',
    'Referer': 'https://dealapp.sa/',
    'User-Agent': USER_AGENT,
    'appversion': '7.2.23'
}

# Browser Args
BROWSER_ARGS = ['--disable-blink-features=AutomationControlled']

# Parameter Combinations
PARAM_COMBINATIONS = [
    # Basic pagination
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'page': 2, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'page': 3, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'page': 4, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'page': 5, 'limit': 10, 'city': '6009d941950ada00061eeeab'},

    # Try with offset
    {'offset': 0, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'offset': 10, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'offset': 20, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'offset': 30, 'limit': 10, 'city': '6009d941950ada00061eeeab'},
    {'offset': 40, 'limit': 10, 'city': '6009d941950ada00061eeeab'},

    # Try with sort
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'sort': 'createdAt'},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'sort': '-createdAt'},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'sort': 'price'},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'sort': '-price'},

    # Try with filters
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'adPurpose': 'SALE'},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'adPurpose': 'RENT'},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'minPrice': 500000},
    {'page': 1, 'limit': 10, 'city': '6009d941950ada00061eeeab', 'maxPrice': 1000000},
]