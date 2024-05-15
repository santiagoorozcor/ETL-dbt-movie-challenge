import os
import sys
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Optional
from helpers.snowflake_helpers import SnowflakeDatabase
from helpers.web_scraping_helpers import table_to_dataframe

# Constants
DATA_DIR = os.path.join(".", "data", "raw")
BOMOJO_MOVIES_RELEASES_FILE = os.path.join(DATA_DIR, "BOMOJO_MOVIES_RELEASES.csv")
BOMOJO_MOVIES_REGIONS_FILE = os.path.join(DATA_DIR, "BOMOJO_MOVIES_REGIONS.csv")
BOMOJO_MOVIES_AREAS_FILE = os.path.join(DATA_DIR, "BOMOJO_MOVIES_AREAS.csv")


def fetch_movie_data(url: str) -> Optional[List[BeautifulSoup]]:
    logging.info(f"Fetching data from {url}")
    try:
        req = requests.get(url)
        req.raise_for_status()  # Raise HTTPError for bad requests

    except (
        requests.exceptions.RequestException,
        requests.exceptions.Timeout,
        requests.exceptions.TooManyRedirects,
    ) as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return None

    soup = BeautifulSoup(req.text, "html.parser")
    tables = soup.find_all("table")
    return tables


def append_to_csv(df: pd.DataFrame, csv_file: str) -> None:
    try:
        if not os.path.exists(csv_file):
            df.to_csv(csv_file, encoding="utf-8", index=False)
        else:
            with open(csv_file, "a", newline="") as file:
                df.to_csv(file, encoding="utf-8", index=False, header=False)
    except IOError as e:
        logging.error(f"Error writing to CSV file {csv_file}: {e}")


def get_countries(df_imdb_id: pd.DataFrame) -> None:
    total_ids = len(df_imdb_id["IMDB_ID"])
    for index, imdb_id in enumerate(df_imdb_id["IMDB_ID"], start=1):
        url = f"https://www.boxofficemojo.com/title/{imdb_id}/"
        tables = fetch_movie_data(url)
        if not tables:
            continue

        # If the movie have more than one release the information is showed with a different structure
        if len(tables) > 1 and tables[1].find_previous_sibling(
            "h3", string="By Release"
        ):
            df_movie_releases = table_to_dataframe(tables[0])
            df_movie_releases["IMDB_ID"] = imdb_id
            append_to_csv(df_movie_releases, BOMOJO_MOVIES_RELEASES_FILE)

            table_start_index = 1
            csv_file_name = BOMOJO_MOVIES_REGIONS_FILE
        else:
            table_start_index = 0
            csv_file_name = BOMOJO_MOVIES_AREAS_FILE

        if len(tables) > 1:
            for table in tables[table_start_index:]:
                df_movie_areas = table_to_dataframe(table)
                df_movie_areas["IMDB_ID"] = imdb_id
                append_to_csv(df_movie_areas, csv_file_name)

        remaining_ids = total_ids - index
        logging.info(f"Processed IMDb ID {imdb_id}. Remaining IDs: {remaining_ids}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: script.py <option>")
        sys.exit(1)

    option = sys.argv[1]

    # Set up logging
    logging.basicConfig(
        filename=f"{option}.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    try:
        db = SnowflakeDatabase(os.path.join(".", "src", "configs"))

        df_imdb_id = db.execute_query(
            """
            SELECT 
                OMDB.IMDB_ID,
                OMDB.TITLE
            FROM
                MOVIE_CHALLENGE.PUBLIC.OMDB_MOVIES OMDB
            WHERE 
                OMDB.BOX_OFFICE IS NOT NULL
            """
        )

        if option == "countries":
            get_countries(df_imdb_id)
        else:
            logging.error(f"Unknown option: {option}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db.close_connection()
        logging.info("Database connection closed. The script has ended.")
