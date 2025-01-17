import logging
from scraper.utils.data_formatter import DataFormatter

logger = logging.getLogger(__name__)

class BaseParser:
    """
    A generic base parser that all specialized parsers (API, HTML, etc.)
    can inherit from. It provides a structure for handling pattern-based
    data extraction and leverages the DataFormatter to standardize data
    across different sources.

    Typical usage involves:
        1) Extracting raw values from a source (e.g., JSON, HTML).
        2) Calling `format_data()` to convert those raw values to the desired types.
        3) Returning the final structured dictionary of parsed data.
    """

    def __init__(self, scraping_instructions=None):
        """
        Initializes the parser with optional scraping instructions.

        Args:
            scraping_instructions (dict, optional): Dictionary of pattern
                definitions or field-specific instructions used during parsing
                and formatting. Defaults to an empty dict if not provided.
        """
        self.scraping_instructions = scraping_instructions or {}
