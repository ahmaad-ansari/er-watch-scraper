from datetime import datetime

import requests
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper
from scraper.parsers.html_parser import HTMLParser
from utils.logger import get_logger

logger = get_logger(__name__)


class HTMLScraper(BaseScraper):
    """
    A scraper class designed to retrieve and parse HTML content
    for 'html' type scraping targets.

    Inherits from:
        BaseScraper (ABC): Provides the abstract interface and a
        finalize_data hook for post-processing.
    """

    def scrape(self):
        """
        Fetches and parses HTML content from the target URL (self.url)
        using the BeautifulSoup library. Then it delegates parsing logic
        to the HTMLParser class, which uses self.scraping_instructions
        for extraction rules.

        Returns:
            dict or None:
                - A dictionary containing standardized data (e.g.,
                  hospital_id, estimated_wait_time, etc.) if parsing
                  succeeds.
                - None if the request fails or parsing yields no result.
        """
        logger.debug(f"Starting HTML scrape for hospital_id={self.hospital_id}, url={self.url}")

        # 1) Send a GET request to the target URL with a 10-second timeout.
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()  # Raises an exception on 4xx/5xx errors
            logger.debug(f"Received HTML response from {self.url} for hospital_id={self.hospital_id}")
        except requests.RequestException as e:
            # Log any request-related issues and return None.
            logger.error(f"Error fetching HTML for hospital_id={self.hospital_id}: {e}")
            return None

        # 2) Parse the HTML response using BeautifulSoup.
        soup = BeautifulSoup(response.text, "html.parser")

        # 3) Use HTMLParser (custom parser) to extract relevant fields
        #    from the soup object based on scraping_instructions.
        parser = HTMLParser(self.scraping_instructions)
        parsed_data = parser.parse(soup)

        return self.process_parsed_data(parsed_data)

