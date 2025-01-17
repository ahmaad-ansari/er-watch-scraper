# base_scraper.py
from abc import ABC, abstractmethod
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class defining the interface and core functionality
    for scraper subclasses that fetch data from various sources.
    """

    def __init__(self, target_info):
        self.hospital_id = target_info.get("hospital_id")
        self.url = target_info.get("url")
        self.scraping_instructions = target_info.get("scraping_instructions", {})

    @abstractmethod
    def scrape(self):
        """
        Subclasses must implement this method to perform their specific
        scraping logic, and return the raw parsed data (dict or None).
        """
        pass

    def process_parsed_data(self, parsed_data: dict) -> dict:
        """
        Centralizes the creation of a final dictionary for DB insertion.

        Args:
            parsed_data (dict): The raw data returned by your parser (e.g., from APIParser or HTMLParser).

        Returns:
            dict or None: The dictionary ready for saving, or None if 'parsed_data' is empty.
        """
        if not parsed_data:
            logger.warning(f"No data parsed for hospital_id={self.hospital_id}")
            return None

        data_for_db = {
            "hospital_id": self.hospital_id,
            "estimated_wait_time": parsed_data.get("estimated_wait_time"),
            "patients_waiting": parsed_data.get("patients_waiting"),
            "patients_in_treatment": parsed_data.get("patients_in_treatment"),
            "last_updated": parsed_data.get("last_updated", datetime.utcnow()),
            "status": "active" if not parsed_data.get("estimated_wait_time") == -1 else "inactive",
        }

        logger.debug(f"Successfully scraped data for hospital_id={self.hospital_id}: {data_for_db}")

        return data_for_db
