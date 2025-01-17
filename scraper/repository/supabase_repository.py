import os
import psycopg2
from datetime import datetime
from typing import List, Dict
from scraper.utils.logger import get_logger

logger = get_logger(__name__)


class SupabaseRepository:
    """
    Manages database interactions for retrieving and storing scraped data.

    This repository class handles:
      1) Establishing a connection to a PostgreSQL database (using psycopg2).
      2) Saving scraped data via an UPSERT operation on the 'scraped_data' table.
      3) Retrieving scraping targets from the 'scraping_targets' table.

    Attributes:
        user (str): Database username from environment variables.
        password (str): Database password from environment variables.
        host (str): Database host from environment variables.
        port (str): Database port from environment variables.
        dbname (str): Database name from environment variables.
        conn (psycopg2.extensions.connection or None): Active database connection, if any.
        cursor (psycopg2.extensions.cursor or None): Cursor for executing SQL commands, if any.
    """

    def __init__(self):
        """
        Initializes the SupabaseRepository by loading environment variables,
        validating database credentials, and establishing a connection to the PostgreSQL DB.

        Raises:
            ValueError: If any required environment variable is missing.
            psycopg2.OperationalError: If unable to connect to the database.
        """
        # Load environment variables from .env file (if present)
        # load_dotenv()

        # Read database connection parameters
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.dbname = os.getenv("DB_NAME")

        # Ensure all variables are loaded
        if not all([self.user, self.password, self.host, self.port, self.dbname]):
            msg = (
                "Database connection details are missing. Check your environment variables "
                "for DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and DB_NAME."
            )
            logger.critical(msg)
            raise ValueError(msg)

        self.conn = None
        self.cursor = None

        try:
            self.conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=self.dbname
            )
            self.cursor = self.conn.cursor()
            logger.info("Database connection established successfully.")
        except psycopg2.OperationalError as e:
            logger.error(f"Error connecting to the database: {e}")
            if self.conn:
                self.conn.rollback()
            raise e

    def save_scraped_data(self, data: Dict) -> None:
        """
        Inserts or updates scraped data in the 'scraped_data' table.

        This method uses an UPSERT query (ON CONFLICT) based on the 'hospital_id' field
        to either insert a new record or update an existing one. It also:
          - Converts 'last_updated' to an ISO8601 string if it's a datetime object.
          - Sets 'updated_at' to the current UTC time (in ISO8601 format).

        Args:
            data (Dict): A dictionary containing the fields to be saved or updated. Expected keys:
                - "hospital_id": str
                - "estimated_wait_time": int or None
                - "patients_waiting": int or None
                - "patients_in_treatment": int or None
                - "last_updated": datetime or str or None
                - "status": str (e.g., "active", "inactive")
        """
        if self.conn is None or self.cursor is None:
            logger.error("Cannot save data: no database connection is available.")
            return

        # Convert datetime to ISO8601 format if needed
        if isinstance(data.get("last_updated"), datetime):
            data["last_updated"] = data["last_updated"].isoformat()

        # Ensure `updated_at` is set to the current UTC time
        data["updated_at"] = datetime.utcnow().isoformat()

        try:
            logger.debug(f"Preparing to UPSERT data for hospital_id={data.get('hospital_id')}: {data}")
            # Create an UPSERT query to insert or update the wait time
            query = """
            INSERT INTO scraped_data 
                (hospital_id, estimated_wait_time, patients_waiting, 
                 patients_in_treatment, last_updated, status, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hospital_id) 
            DO UPDATE SET 
                estimated_wait_time     = EXCLUDED.estimated_wait_time,
                patients_waiting        = EXCLUDED.patients_waiting,
                patients_in_treatment   = EXCLUDED.patients_in_treatment,
                last_updated            = EXCLUDED.last_updated,
                status                  = EXCLUDED.status,
                updated_at              = EXCLUDED.updated_at;
            """
            self.cursor.execute(
                query,
                (
                    data["hospital_id"],
                    data["estimated_wait_time"],
                    data["patients_waiting"],
                    data["patients_in_treatment"],
                    data["last_updated"],
                    data["status"],
                    data["updated_at"],
                )
            )
            self.conn.commit()
            logger.info(f"Data saved successfully for hospital_id={data['hospital_id']}.")
        except Exception as e:
            logger.error(f"Error saving data to the database for hospital_id={data.get('hospital_id')}: {e}")
            self.conn.rollback()

    def get_target_data(self) -> List[Dict]:
        """
        Retrieves target entries from the 'scraping_targets' table.

        This table typically defines the scraping instructions (e.g., which URL to scrape,
        action type, or other metadata).

        Returns:
            List[Dict]: A list of dictionaries, each representing a row from 'scraping_targets',
                or an empty list if no rows are found or an error occurs.
        """
        if self.conn is None or self.cursor is None:
            logger.error("Cannot retrieve target data: no database connection is available.")
            return []

        try:
            query = "SELECT * FROM scraping_targets;"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            if not rows:
                logger.info("No rows found in 'scraping_targets' table.")
                return []

            columns = [desc[0] for desc in self.cursor.description]
            logger.info(f"Fetched {len(rows)} rows from 'scraping_targets' table.")

            # Convert each row to a dictionary keyed by column name
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)

            return data

        except Exception as e:
            logger.error(f"Error downloading target data: {e}")
            # Depending on your use case, you might raise e or return []
            return []

    def __del__(self):
        """
        Ensures the database cursor and connection are closed gracefully
        when the SupabaseRepository object is garbage-collected.
        """
        if self.cursor:
            self.cursor.close()
            logger.debug("Database cursor closed.")
        if self.conn:
            self.conn.close()
            logger.debug("Database connection closed.")
