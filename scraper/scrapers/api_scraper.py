from datetime import datetime

import requests
from .base_scraper import BaseScraper
from scraper.parsers.api_parser import APIParser
from scraper.utils.logger import get_logger

logger = get_logger(__name__)


class APIScraper(BaseScraper):
    """
    A scraper class designed to retrieve data from an API endpoint
    and parse the result into a consistent data structure.

    Inherits from:
        BaseScraper (ABC): Provides the abstract interface and common
        data finalization logic via the `finalize_data` method.
    """

    def scrape(self):
        """
        Fetches data from the API endpoint (self.url) using a GET request,
        then determines the response content type and parses the data
        accordingly. If the server returns JSON, it is parsed as JSON;
        otherwise, the raw text content is parsed.

        Returns:
            dict or None:
                - A dictionary containing standardized data (e.g., hospital_id,
                  estimated_wait_time, etc.) if parsing is successful.
                - None if the request fails or no data can be parsed.
        """
        logger.debug(f"Attempting to fetch API data from {self.url}")

        # 1) Make the request to the specified URL with a 10-second timeout.
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()  # Raises an exception if status is 4xx/5xx
            logger.debug(f"Received response from {self.url} for hospital_id={self.hospital_id}")
        except requests.RequestException as e:
            logger.error(f"Error fetching data for hospital_id={self.hospital_id}: {e}")
            return None

        # 2) Inspect the Content-Type header to determine the response format.
        content_type = response.headers.get("Content-Type", "").lower()

        # 3) Select the parsing approach based on JSON vs. text content.
        #    If the body starts with '{' or '[', we assume it's JSON,
        #    even if the content-type doesn't explicitly say so.
        if ("application/json" in content_type or
            response.text.strip().startswith("{") or
            response.text.strip().startswith("[")):
            # Parse as JSON using the APIParser.
            try:
                json_data = response.json()
                parsed_data = APIParser(self.scraping_instructions).parse(json_data)
                logger.debug(f"Parsed JSON data for hospital_id={self.hospital_id}: {parsed_data}")
            except ValueError as parse_err:
                logger.error(f"Failed to parse JSON response for hospital_id={self.hospital_id}: {parse_err}")
                return None
        else:
            # Otherwise, parse as plain text.
            parsed_data = APIParser(self.scraping_instructions).parse_plain_text(response.text)
            logger.debug(f"Parsed plain text data for hospital_id={self.hospital_id}: {parsed_data}")

        return self.process_parsed_data(parsed_data)