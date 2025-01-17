import asyncio
import json
import os
from pathlib import Path

from aggregator import Aggregator
from utils.logger import get_logger
from repository.supabase_repository import SupabaseRepository


logger = get_logger(__name__)

async def main():
    """
    Main entry point for running the scraper. It:
      1. Saves target data to a JSON file.
      2. Reads the JSON data containing scraping targets.
      3. Instantiates the aggregator and processes each target.
    """
    logger.info("Starting the scraper...")

    # Step 1: Save the target data to a JSON file for subsequent use.
    save_target_data_to_json()

    # Step 2: Load the saved scraping targets from the JSON file.
    scraping_targets_path = Path(__file__).parent.parent / "data" / "scraping_targets_data.json"
    try:
        with open(scraping_targets_path, "r", encoding="utf-8") as f:
            scraping_targets = json.load(f)
        logger.debug(f"Loaded {len(scraping_targets)} scraping targets from {scraping_targets_path}.")
    except FileNotFoundError:
        # Log an error if the file is missing and exit early.
        logger.error(f"scraping_targets_data.json not found at {scraping_targets_path}")
        return
    except json.JSONDecodeError as e:
        # Log an error if there's an issue with JSON decoding.
        logger.error(f"JSON decoding error for {scraping_targets_path}: {e}")
        return

    # Step 3: Instantiate the aggregator with the loaded targets and run the process.
    aggregator = Aggregator(scraping_targets)
    logger.info("Running the aggregator...")
    await aggregator.run()
    logger.info("Scraper run complete.")


def save_target_data_to_json():
    """
    Retrieves target data from the Supabase repository and saves it to a local JSON file.

    Raises:
        Exception: If an unexpected error occurs during the data retrieval or file operations.
    """
    logger.info("Starting the process to save target data to JSON...")

    try:
        # Create a SupabaseRepository instance to fetch the target data.
        repo = SupabaseRepository()
        target_data = repo.get_target_data()

        if not target_data:
            # Log a warning if no data was retrieved.
            logger.warning("No target data retrieved from the repository.")
            return

        # Create the data directory if it does not already exist.
        data_dir = os.path.join(os.path.dirname(__file__), '../data')
        os.makedirs(data_dir, exist_ok=True)

        # Define the output path for the JSON file.
        output_path = os.path.join(data_dir, 'scraping_targets_data.json')

        # Write the target data to the JSON file.
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(target_data, json_file, indent=4, ensure_ascii=False)
        logger.info(f"Data successfully saved to {output_path}")

    except Exception as e:
        # Log any exception that occurs, including stack trace.
        logger.error(f"Error occurred while saving target data to JSON: {e}", exc_info=True)


if __name__ == "__main__":
    # Only run the script if called directly (not imported).
    asyncio.run(main())
