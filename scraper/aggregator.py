from scrapers.api_scraper import APIScraper
from scrapers.html_scraper import HTMLScraper
from scrapers.pbi_scraper import PBIScraper
from repository.supabase_repository import SupabaseRepository
from utils.logger import get_logger

logger = get_logger(__name__)


class Aggregator:
    """
    The Aggregator class orchestrates multiple scrapers to retrieve data
    from various sources and store the results using a Supabase repository.

    Attributes:
        scraping_targets (list): A list of dictionaries that define how and
            what to scrape. Each dictionary typically contains keys like
            "action" and "hospital_id".
        supabase_repo (SupabaseRepository): An instance of the SupabaseRepository
            for saving the scraped data.
    """

    def __init__(self, scraping_targets):
        """
        Initializes the Aggregator with the scraping targets.

        Args:
            scraping_targets (list): A list of dictionaries specifying
                scraping parameters, including the "action" type and other details.
        """
        self.scraping_targets = scraping_targets
        try:
            self.supabase_repo = SupabaseRepository()
            logger.info("SupabaseRepository initialized successfully.")
        except ValueError as e:
            # This typically indicates missing environment variables or other init issues.
            logger.critical(f"Failed to initialize SupabaseRepository: {e}")
            self.supabase_repo = None

    async def run(self):
        """
        Executes the scraping workflow by:
          1. Validating the Supabase repository is initialized.
          2. Iterating through the scraping targets and selecting the appropriate scraper.
          3. Storing the scraped data via the Supabase repository.

        If the repository is not initialized, the process will be halted.
        """
        # Check if the Supabase repository was successfully set up.
        if not self.supabase_repo:
            logger.error("Cannot proceed without a valid SupabaseRepository instance.")
            return

        logger.info("Starting the Aggregator run...")

        # Iterate over each scraping target and process accordingly.
        for target in self.scraping_targets:
            action = target.get("action")
            hospital_id = target.get("hospital_id")

            logger.debug(f"Processing target: hospital_id={hospital_id}, action={action}")

            # Select the scraper type based on the "action" field.
            try:
                if action == "api":
                    scraper = APIScraper(target)
                    data = scraper.scrape()
                elif action == "html":
                    scraper = HTMLScraper(target)
                    data = scraper.scrape()
                elif action == "pbi":
                    scraper = PBIScraper(target)
                    data = await scraper.scrape()
                elif action == "pbi_h":
                    scraper = PBIScraper(target)
                    data = await scraper.scrape(use_headers=True)
                else:
                    # Log a warning if the action type is not supported.
                    logger.warning(f"Unsupported action type for hospital_id={hospital_id}: '{action}'")
                    continue
            except Exception as e:
                # Log any scraper-related exception, then move to the next target.
                logger.error(f"An error occurred while scraping hospital_id={hospital_id}: {e}", exc_info=True)

                # Create a minimal data dictionary marking the row as inactive
                error_data = {
                    "hospital_id": hospital_id,
                    "estimated_wait_time": None,
                    "patients_waiting": None,
                    "patients_in_treatment": None,
                    "last_updated": None,
                    "status": "inactive",
                }

                # Save that data to set the row's status to "inactive"
                self.supabase_repo.save_scraped_data(error_data)
                continue

            # Check if any data was returned by the scraper.
            if not data:
                logger.warning(f"No data scraped for hospital_id={hospital_id}.")
                continue

            # Log the successful scrape result and save the data.
            logger.info(f"Scraped data for hospital_id={hospital_id}: {data}")
            self.supabase_repo.save_scraped_data(data)

        logger.info("Aggregator run complete.")
