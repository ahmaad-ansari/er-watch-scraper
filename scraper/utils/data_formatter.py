import logging
import re
from datetime import datetime
import pytz

from scraper.utils.logger import get_logger

logger = get_logger(__name__)


class DataFormatter:
    """
    A utility class for converting raw field values into standardized types/formats
    (e.g., date/time objects, integers, total minutes).

    Example Fields the Formatter Handles:
    - lastUpdated
    - patientsWaiting
    - patientsInTreatment
    - estimatedWaitTime

    The specific field determines what kind of parsing logic is used.
    """

    @staticmethod
    def format_value(field, format_code, raw_value, pattern, unit):
        """
        Parse or transform the 'raw_value' based on 'field', 'format_code', and 'pattern'.
        This method standardizes various types of input data such as date/time strings,
        numeric counts, and time durations into more usable Python objects.

        Args:
            field (str): The name of the field to format (e.g., 'lastUpdated', 'patientsWaiting').
            format_code (str): A datetime format string to guide parsing, if needed.
            raw_value (str or None): The original, unprocessed data.
            pattern (str or None): A regex pattern for extracting data from the 'raw_value'.
            unit (str or None): A unit of measurement (e.g., 'minutes', 'hours') when relevant.

        Returns:
            datetime | int | str | None:
                - A datetime object if 'lastUpdated' is successfully parsed.
                - An integer if numeric fields or wait times are parsed.
                - The original 'raw_value' if the field is unrecognized (with a warning).
                - None if parsing or conversion fails.
        """
        logger.debug(f"Starting format_value for field='{field}', raw_value='{raw_value}'")

        # If there's no value to process, log a warning and return None.
        if raw_value is None:
            logger.warning(f"Field '{field}' has no value. Returning None.")
            return None

        # ---------------------------------------------------------------------
        # 1) Handle "lastUpdated" (Date/Time Parsing)
        # ---------------------------------------------------------------------
        if field == "lastUpdated":
            logger.debug(f"Processing 'lastUpdated' with raw_value='{raw_value}', pattern='{pattern}'")
            match = re.search(pattern, raw_value)

            if match:
                date_time_str = match.group(1).strip()
                logger.debug(f"Extracted datetime string: '{date_time_str}' from raw_value='{raw_value}'")

                try:
                    # Normalize ordinal suffixes (e.g., 1st, 2nd, etc.) and AM/PM
                    normalized_str = re.sub(r"(st|nd|rd|th)", "", date_time_str)
                    # Convert "p.m." or "a.m." to uppercase AM/PM without dots
                    normalized_str = re.sub(
                        r"\b(p\.m\.?|a\.m\.?)",
                        lambda m: m.group(0).replace(".", "").upper(),
                        normalized_str
                    )

                    # Truncate microseconds to 6 digits if present.
                    normalized_str = re.sub(r"(\.\d{6})\d+", r"\1", normalized_str)

                    logger.debug(f"Normalized datetime string: '{normalized_str}'")

                    # 1) Attempt to parse the datetime using the provided format_code.
                    dt = datetime.strptime(normalized_str, format_code)

                    # 2) If day/month is missing in format_code, replace them with current date components.
                    now = datetime.now()
                    if "%d" not in format_code:
                        dt = dt.replace(day=now.day)
                    # If none of the format specifiers for month exist, fill in the current month.
                    if all(x not in format_code for x in ["%m", "%b", "%B"]):
                        dt = dt.replace(month=now.month)

                    # 3) If the year is missing, default to the current year.
                    if "%Y" not in format_code:
                        dt = dt.replace(year=now.year)

                    # 4) Check if the format indicates UTC via 'Z'; if so, assign or convert to UTC.
                    if "%fZ" in format_code or "Z" in format_code:
                        # If there's a literal "Z" in the format, we interpret the string as UTC.
                        dt = dt.replace(tzinfo=pytz.UTC)
                    else:
                        # Otherwise, treat the datetime as local and convert to UTC.
                        dt = dt.replace(tzinfo=None)
                        dt = dt.astimezone(datetime.utcnow().astimezone(pytz.UTC).tzinfo)

                    # 5) Ensure microseconds are capped at 6 digits; Python does this by default after parsing.
                    dt = dt.replace(microsecond=dt.microsecond)

                    logger.info(f"Successfully parsed 'lastUpdated' (UTC, 6-digit micros): '{dt}'")
                    return dt

                except ValueError as e:
                    logger.error(f"Failed to parse datetime for field='{field}'. Error: {e}")
                    return None
            else:
                logger.warning(f"No match found for field='{field}' using pattern='{pattern}'. "
                               "Returning None.")
                return None

        # ---------------------------------------------------------------------
        # 2) Handle "patientsWaiting" or "patientsInTreatment" (Integer Parsing)
        # ---------------------------------------------------------------------
        elif field in ["patientsWaiting", "patientsInTreatment"]:
            logger.debug(f"Processing numeric field='{field}' with raw_value='{raw_value}'")
            try:
                # Convert the string to an integer.
                result = int(raw_value.strip())
                logger.info(f"Successfully converted '{raw_value}' to integer: {result}")
                return result
            except ValueError as e:
                logger.error(f"Failed to convert raw_value='{raw_value}' to int for field='{field}'. Error: {e}")
                return None

        # ---------------------------------------------------------------------
        # 3) Handle "estimatedWaitTime" (Time Duration Parsing)
        # ---------------------------------------------------------------------
        elif field == "estimatedWaitTime":
            logger.debug(
                f"Processing 'estimatedWaitTime' with raw_value='{raw_value}', pattern='{pattern}', unit='{unit}'"
            )

            if unit == "minutes":
                # Directly convert the raw_value to an integer (presumably minutes).
                try:
                    result = round(int(float(raw_value)))
                    logger.info(f"Converted '{raw_value}' to minutes: {result}")
                    return result
                except ValueError as e:
                    logger.error(f"Failed to convert raw_value='{raw_value}' to minutes. Error: {e}")
                    return None

            elif unit == "hours":
                # First attempt direct conversion (assuming raw_value is in hours).
                try:
                    result = round(int(float(raw_value) * 60))
                    logger.info(f"Converted '{raw_value}' from hours to minutes: {result}")
                    return result
                except ValueError:
                    logger.debug(
                        f"Direct conversion of raw_value='{raw_value}' to minutes failed. "
                        "Attempting regex pattern match."
                    )

                # Use regex to extract hours if direct conversion fails.
                match = re.match(pattern, raw_value)
                if match:
                    try:
                        hours = float(match.group(1))
                        minutes = hours * 60
                        result = round(minutes)
                        logger.info(f"Extracted '{raw_value}' using regex and converted to minutes: {result}")
                        return result
                    except ValueError as e:
                        logger.error(f"Failed to extract hours from raw_value='{raw_value}'. Error: {e}")
                        return None
                else:
                    logger.warning(f"Regex failed to match raw_value='{raw_value}' for field='{field}'.")
                    return None

            else:
                # If 'unit' is not recognized or it's None, try parsing "X hours and Y minutes" style.
                match = re.match(pattern, raw_value)
                if match:
                    try:
                        hours = int(match.group(1))
                        minutes = int(match.group(2))
                        total_minutes = hours * 60 + minutes
                        logger.info(f"Extracted '{raw_value}' as {total_minutes} total minutes.")
                        return total_minutes
                    except ValueError as e:
                        logger.error(f"Failed to parse hours/minutes from raw_value='{raw_value}'. Error: {e}")
                        return None
                else:
                    logger.warning(f"Invalid format for 'estimatedWaitTime': '{raw_value}'. Returning None.")
                    return None

        # ---------------------------------------------------------------------
        # 4) Handle Unrecognized Fields
        # ---------------------------------------------------------------------
        logger.warning(
            f"Field '{field}' not recognized. Returning raw_value='{raw_value}' without modification."
        )
        return raw_value


if __name__ == "__main__":
    # Example usage and test cases to demonstrate the formatter's functionality.

    test_cases = [
        {
            "lastUpdated": {
                "pattern": "^([A-Za-z]+ \\d{1,2}, \\d{4}) at (\\d{1,2}:\\d{2} (a\\.m\\.|p\\.m\\.))$",
                "format_code": "%B %d, %Y",
                "raw_value": "January 24, 2025 at 2:30 p.m.",

                "unit": None
            },
            "patientsWaiting": {
                "format_code": None,
                "pattern": None,
                "raw_value": "17",
                "unit": None
            },
            "estimatedWaitTime": {
                "format_code": None,
                "pattern": r"(\d+)\s*hour[s]?\s*and\s*(\d+)\s*minute[s]?",
                "raw_value": "2 hours and 41 minutes",
                "unit": None
            },
            "patientsInTreatment": {
                "format_code": None,
                "pattern": None,
                "raw_value": None,
                "unit": None
            },
            # The "response" key is used only for test-checking the returned data.
            "response": "[datetime.datetime(2025, 1, 16, 23, 15), 17, 161, None]"
        }
    ]

    formatter = DataFormatter()

    for index, test_case in enumerate(test_cases, start=1):
        response = []
        # Iterate over each field in the test case, except 'response' which is just for comparison.
        for field, info in test_case.items():
            if field == "response":
                continue

            result = formatter.format_value(
                field=field,
                format_code=info.get("format_code"),
                raw_value=info.get("raw_value"),
                pattern=info.get("pattern"),
                unit=info.get("unit")
            )
            response.append(result)

        # Compare the actual result to the expected 'response' string.
        expected = test_case["response"]

        print(f"Test case {index}: ", end='')
        if str(response) == expected:
            print("PASS")
        else:
            print("FAIL")
            print(f"  Expected: {expected}")
            print(f"  Got:      {response}")
