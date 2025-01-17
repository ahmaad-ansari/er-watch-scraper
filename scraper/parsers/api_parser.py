from .base_parser import BaseParser
from scraper.utils.logger import get_logger
from scraper.utils.data_formatter import DataFormatter

logger = get_logger(__name__)


class APIParser(BaseParser):
    """
    A parser specialized for JSON and plain-text data originating from API responses.
    Inherits from BaseParser to leverage optional shared functionality.

    This class uses field-specific instructions (stored in 'scraping_instructions')
    to:
      1) Identify and extract data from JSON keys (with optional list indices).
      2) Convert raw strings into standardized types (e.g., integers, datetimes)
         via DataFormatter.
      3) Map parsed fields to a final schema (e.g., 'lastUpdated' → 'last_updated').
    """

    def parse(self, data):
        """
        Parses JSON data using the instructions in 'scraping_instructions'.

        Each field instruction may contain:
          - 'dataPath': A string path indicating how to navigate nested JSON objects/lists.
          - 'formatCode': A datetime format string (or other code) to guide parsing.
          - 'pattern': A regex pattern for advanced text matching.
          - 'unit': A unit of measure (e.g., 'minutes' or 'hours') for time-related parsing.

        Workflow:
          1) Validate that 'data' is not empty.
          2) For each field in 'scraping_instructions', extract a raw value via '_extract_data'.
          3) Convert the raw value to a string if necessary, then parse it with DataFormatter.
          4) Map the parsed result to a final schema key (e.g., 'lastUpdated' → 'last_updated').

        Args:
            data (dict): The JSON data to parse (e.g., the result of 'json.loads()').

        Returns:
            dict or None: A dictionary of parsed fields or None if no data was provided.
        """
        if not data:
            logger.error("APIParser received no JSON data to parse.")
            return None

        result = {}

        for key, field_instructions in self.scraping_instructions.items():
            # Extract relevant instructions
            data_path = field_instructions.get("dataPath")
            format_code = field_instructions.get("formatCode")
            pattern = field_instructions.get("pattern")
            unit = field_instructions.get("unit")

            # 1) Extract raw value from JSON using the data path
            raw_value = self._extract_data(data, data_path)

            # 2) Convert the raw value to a string if it's not None
            raw_str = str(raw_value) if raw_value is not None else None

            # 3) Parse/format the extracted string using DataFormatter
            parsed_value = DataFormatter.format_value(
                field=key,
                format_code=format_code,
                raw_value=raw_str,
                pattern=pattern,
                unit=unit
            )

            # 4) Map the parsed value to your final schema
            if key == "lastUpdated":
                result["last_updated"] = parsed_value
            elif key == "patientsWaiting":
                result["patients_waiting"] = parsed_value
            elif key == "patientsInTreatment":
                result["patients_in_treatment"] = parsed_value
            elif key == "estimatedWaitTime":
                result["estimated_wait_time"] = parsed_value
            else:
                # Fallback for fields not explicitly covered
                result[key] = parsed_value

        logger.debug(f"APIParser result: {result}")
        return result

    def parse_plain_text(self, text_data):
        """
        Parses raw text (non-JSON) data, typically a single-line or simple multiline string.

        Each field instruction in 'scraping_instructions' may still include:
          - 'formatCode': A string for date/time parsing or other format instructions.
          - 'pattern': A regex pattern for matching relevant parts of the text.
          - 'unit': A unit of measure (e.g., 'minutes' or 'hours') for time-related conversions.

        Since this is plain text, 'dataPath' is generally irrelevant, but
        it's retained for compatibility with the JSON-based approach.

        Workflow:
          1) Verify 'text_data' is not empty.
          2) For each field, ignore 'dataPath' and treat the entire text as 'raw_value'.
          3) Pass the raw value to DataFormatter for parsing.
          4) Map the parsed result to the final schema.

        Args:
            text_data (str): The plain text content to parse.

        Returns:
            dict or None: A dictionary of parsed fields or None if 'text_data' is empty.
        """
        if not text_data:
            logger.error("No text_data provided to parse_plain_text.")
            return None

        result = {}
        for key, field_instructions in self.scraping_instructions.items():
            # Extract relevant instructions
            data_path = field_instructions.get("dataPath")
            format_code = field_instructions.get("formatCode")
            pattern = field_instructions.get("pattern")
            unit = field_instructions.get("unit")

            # In plain text mode, ignore 'dataPath'—just parse the entire text
            raw_str = text_data.strip() if text_data else None

            parsed_value = DataFormatter.format_value(
                field=key,
                format_code=format_code,
                raw_value=raw_str,
                pattern=pattern,
                unit=unit
            )

            # Map the parsed value to final schema
            if key == "lastUpdated":
                result["last_updated"] = parsed_value
            elif key == "patientsWaiting":
                result["patients_waiting"] = parsed_value
            elif key == "patientsInTreatment":
                result["patients_in_treatment"] = parsed_value
            elif key == "estimatedWaitTime":
                result["estimated_wait_time"] = parsed_value
            else:
                result[key] = parsed_value

        logger.debug(f"APIParser plain text result: {result}")
        return result

    def _extract_data(self, data, data_path):
        """
        Safely extracts a value from a JSON-like structure using a dot/bracket path.

        Examples:
          - data_path='sites[0].lastUpdate' => data["sites"][0]["lastUpdate"]
          - If data_path is empty, returns None and logs a warning.

        Args:
            data (dict): The JSON data from which values are extracted.
            data_path (str): A bracket/dot notation string specifying nested keys/indices.

        Returns:
            Any or None: The extracted value if the path is valid, otherwise None.
        """
        if not data_path:
            logger.warning("No dataPath provided.")
            return None

        try:
            # Replace bracket notation with dots and split
            parts = [p for p in data_path.replace("[", ".").replace("]", "").split(".") if p]
            cur = data
            # Traverse the JSON structure according to the parts
            for part in parts:
                if part.isdigit():  # Handle numeric list indices
                    part = int(part)
                logger.debug(f"Accessing part='{part}' in data='{cur}'")
                cur = cur[part]
            return cur

        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Failed to extract data using path '{data_path}': {e}")
            return None
