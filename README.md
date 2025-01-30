# ER Watch
[![Run My Scraper](https://github.com/ahmaad-ansari/er-watch-scraper/actions/workflows/scraper.yml/badge.svg)](https://github.com/ahmaad-ansari/er-watch-scraper/actions/workflows/scraper.yml)

**ER Watch** is a real-time platform that provides accurate emergency room wait times for major hospitals across Ontario. By dynamically scraping or querying various online sources (including APIs, HTML pages, and Power BI dashboards), **ER Watch** allows users to quickly check and compare wait times, helping them make informed decisions about where to seek urgent care.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Key Components](#key-components)
4. [Setup & Installation](#setup--installation)
5. [Usage](#usage)
6. [Data Flow](#data-flow)
7. [Environment Variables](#environment-variables)
8. [Extending the Project](#extending-the-project)
9. [License](#license)

---

## Overview

### Key Goals

- **Real-Time Data**: Collects the latest ER wait times from multiple sources.  
- **Consistency & Flexibility**: Common data formatting and modular scrapers ensure new hospitals or sources can be added easily.  
- **Reliable Persistence**: Stores scraped data in a PostgreSQL (Supabase) table with an UPSERT pattern, guaranteeing current wait times.  
- **Maintainable Architecture**: Organized into logical modules (Scrapers, Parsers, Repository, Utils).

---

## Project Structure

Below is the typical layout of the **ER Watch** repository:

```
er-watch-scraper/
├── scraper/
│   ├── __init__.py
│   ├── main.py
│   ├── aggregator.py
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base_parser.py
│   │   ├── api_parser.py
│   │   └── html_parser.py
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── base_scraper.py
│   │   ├── api_scraper.py
│   │   ├── html_scraper.py
│   │   └── pbi_scraper.py
│   ├── repository/
│   │   ├── __init__.py
│   │   └── supabase_repository.py
│   └── utils/
│       ├── __init__.py
│       ├── logger.py
│       └── data_formatter.py
├── data/
│   └── scraping_targets_data.json
├── requirements.txt
├── .env
└── README.md
```

### What Each Directory Does

- **`scraper/`**: Main application package containing all the scraping logic and supporting modules.  
- **`scraper/main.py`**: Entry point for the entire scraping process—reads the JSON, orchestrates scrapers.  
- **`scraper/aggregator.py`**: Iterates over each target (from the database or JSON file) and invokes the correct scraper.  
- **`scraper/parsers/`**: Handles how scraped data is parsed or mapped, such as JSON-to-dict (`api_parser.py`) or HTML-to-dict (`html_parser.py`).  
- **`scraper/scrapers/`**: Contains the actual scraper classes, which fetch data from various sources:  
  - **`base_scraper.py`**: Abstract class specifying the scraper interface.  
  - **`api_scraper.py`**: Retrieves data from API endpoints and integrates with `api_parser.py`.  
  - **`html_scraper.py`**: Scrapes HTML pages using `BeautifulSoup`.  
  - **`pbi_scraper.py`**: Uses Playwright to handle JavaScript-driven pages (e.g., Power BI dashboards).  
- **`scraper/repository/`**: Manages database interactions (`supabase_repository.py`).  
- **`scraper/utils/`**: Shared utilities such as:  
  - **`logger.py`**: Sets up a color-coded logger.  
  - **`data_formatter.py`**: Normalizes or parses raw field values (e.g., convert “2.5 hours” → 150 minutes).  
- **`data/`**: Stores JSON files containing scraping targets.  
- **`.env`**: Contains environment variables for database credentials and other configs.  
- **`requirements.txt`**: List of Python dependencies.  

---

## Key Components

1. **Main Script (`main.py`)**  
   - **Role**: Primary orchestration—loads targets from the database or `data/scraping_targets_data.json`, invokes the `Aggregator`, and logs the results.

2. **Aggregator (`aggregator.py`)**  
   - **Role**: Loops through each scraping target, determines which scraper to use (API, HTML, PBI), calls the appropriate scraper, then saves data.

3. **Scrapers**  
   - **`api_scraper.py`**: Fetches JSON or plain text data from REST endpoints, optionally uses an `APIParser` for deeper parsing.  
   - **`html_scraper.py`**: Uses `requests` + `BeautifulSoup` to parse static HTML pages.  
   - **`pbi_scraper.py`**: Leverages Playwright for dynamic content (e.g., Power BI embedded pages).  
   - **`base_scraper.py`**: Abstract base class that all scrapers inherit, providing a consistent interface (`scrape()` method).

4. **Parsers**  
   - **`api_parser.py`**: Takes raw JSON or text responses, maps the data using instructions, and standardizes it.  
   - **`html_parser.py`**: Examines the HTML DOM (via `BeautifulSoup`), matches elements using selectors, extracts text, and formats it.

5. **Repository (`supabase_repository.py`)**  
   - **Role**: Establishes a PostgreSQL (Supabase) connection, performs UPSERT operations on the `scraped_data` table, and retrieves scraping targets from `scraping_targets`.

6. **Utilities**  
   - **`data_formatter.py`**: Provides `format_value(...)` for converting times, dates, or numeric strings into typed values.  
   - **`logger.py`**: Configures a color-coded logger for consistent logging throughout the project.

7. **`scraping_targets_data.json`**  
   - **Role**: Defines the scraping instructions for each hospital or endpoint if a database approach is not used or for quick debugging/demo.

---

## Setup & Installation

1. **Clone the Repository**  
   ```bash
   git clone https://github.com/ahmaad-ansari/er-watch-scraper.git
   cd er-watch-scraper
   ```

2. **(Optional) Create a Virtual Environment**  
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables**  
   - Create a `.env` file in the project root, setting at least the following:
     ```
     DB_USER=your_db_user
     DB_PASSWORD=your_db_password
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=your_db_name
     ```
   - You may need additional environment variables for advanced setups.

5. **Check Your Database Setup**  
   - Ensure a PostgreSQL database is running and the `DB_*` environment variables match your credentials.  
   - Create the required tables:
     - **`scraping_targets`**: Where each row contains metadata like `hospital_id`, `action` (“api”, “html”, “pbi”), `url`, and any relevant parsing instructions.  
     - **`scraped_data`**: Used to store the current data (e.g., `hospital_id`, `estimated_wait_time`, `patients_waiting`, `patients_in_treatment`, `last_updated`, `status`, `updated_at`).

---

## Usage

1. **Populate `scraping_targets`**  
   - If you’re pulling targets from the database, insert rows with each hospital’s details.  
   - Alternatively, edit `data/scraping_targets_data.json` for a quick local test.

2. **Run the Main Script**  
   ```bash
   python -m scraper.main
   ```
   - The script will:
     1. Retrieve the scraping targets (from DB or JSON).  
     2. For each target, select the appropriate scraper based on `action`.  
     3. Scrape and parse the data, then save it to your PostgreSQL database (via `SupabaseRepository`).

3. **Monitor Logs**  
   - By default, logs are color-coded and shown on the console. Check the output for:
     - **INFO**: Status updates (connected to DB, completed scraping, etc.).  
     - **WARNING**: Potential issues (e.g., missing data, empty results).  
     - **ERROR** or **CRITICAL**: Something went wrong (DB connection failed, parse error, etc.).

---

## Data Flow

1. **Fetching Targets**: `SupabaseRepository.get_target_data()` (or reading `scraping_targets_data.json`).  
2. **Aggregator**: Iterates over targets, calls `APIScraper`, `HTMLScraper`, or `PBIScraper`.  
3. **Scraping & Parsing**:  
   - **`scrape()`** method fetches raw content.  
   - Parsing logic (e.g., `APIParser` or `HTMLParser`) converts raw fields into standardized data.  
   - `data_formatter.py` ensures consistent formatting (dates, integers, durations).  
4. **Saving**: `SupabaseRepository.save_scraped_data()` performs an **UPSERT** in the `scraped_data` table.  
5. **Result**: The latest hospital wait times are now stored in the database, ready for downstream services or dashboards.

---

## Environment Variables

**`.env` Example**:
```
DB_USER=yourUser
DB_PASSWORD=yourPassword
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=yourDatabase
```

- **Additional Variables**: If you have custom headers for scraping, specialized API keys, or Playwright configurations, include them here and read them in your scraper classes or config modules.

---

## Extending the Project

1. **Add a New Scraper**  
   - Create a new file (e.g., `my_new_scraper.py`) inheriting from `BaseScraper`.  
   - Implement the `scrape()` method using the best approach (requests, playwright, etc.).  
   - Update the `aggregator.py` logic to handle the new `action` type.

2. **Add a New Parser**  
   - Create a file in `scraper/parsers/` (e.g., `my_new_parser.py`).  
   - Handle any specialized parsing logic, then integrate it with your new scraper or aggregator.

3. **Refine Database Tables**  
   - Alter or extend `scraped_data` to store additional metrics (e.g., bed capacity, triage levels).  
   - Update `SupabaseRepository.save_scraped_data()` to match.

4. **Enhance Data Formatting**  
   - Add or modify methods in `data_formatter.py` to handle new data types, complex date/time strings, or error handling.

---

## License

This project does not currently specify a license. For more information or if you wish to use this in a commercial or open-source context, please contact the repository owner.

---

**Thank you for using ER Watch!**  
If you encounter any issues or have suggestions, please open an issue or submit a pull request. We welcome feedback and contributions to help make emergency care more accessible and efficient for GTA residents.
