import os
import sys
import logging
import pandas as pd
from typing import List
from pathlib import Path
from dotenv import load_dotenv
from helpers.snowflake_helpers import SnowflakeDatabase
from helpers import (
    PRO_BOMOJO_RELEASES_FILE,
    PRO_BOMOJO_COUNTRIES_FILE,
    PRO_BOMOJO_BRANDS_FILE,
    PRO_BOMOJO_FRANCHISES_FILE,
    PRO_MOVIES_AWARDS_FILE,
)


load_dotenv()


def load_data(
    db: SnowflakeDatabase, file_path: Path, desired_order: List, table_name: str
) -> None:
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        logging.error(f"Error reading parquet file: {e}")
        return

    df = df.reindex(columns=desired_order)

    try:
        db.load_data(df, table_name)
        logging.info(f"Data successfully loaded into {table_name}")
    except Exception as e:
        print(f"Error loading data into the database: {e}")


def main():
    if len(sys.argv) < 2:
        print("Usage: script.py <option>")
        sys.exit(1)

    option = sys.argv[1]

    # Set up logging
    logging.basicConfig(
        filename=f"loader_{option}.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    logging.info("Script started with option: %s", option)

    try:
        db = SnowflakeDatabase(
            os.getenv("USER"),
            os.getenv("PASSWORD"),
            os.getenv("ACCOUNT"),
            os.getenv("WAREHOUSE"),
            os.getenv("PERSONAL_DATABASE"),
            os.getenv("PERSONAL_SCHEMA"),
        )

        if option == "releases":
            desired_order = [
                "IMDB_ID",
                "RELEASE_GROUP",
                "ROLLOUT",
                "MARKETS",
                "DOMESTIC",
                "INTERNATIONAL",
                "WORLDWIDE",
            ]
            load_data(db, PRO_BOMOJO_RELEASES_FILE, desired_order, "BOMOJO_RELEASES")
        elif option == "countries":
            desired_order = [
                "IMDB_ID",
                "AREA",
                "REGION",
                "RELEASES",
                "LIFETIME_GROSS",
            ]
            load_data(db, PRO_BOMOJO_COUNTRIES_FILE, desired_order, "BOMOJO_COUNTRIES")
        elif option == "brands":
            desired_order = [
                "BRAND",
                "IMDB_ID",
            ]
            load_data(db, PRO_BOMOJO_BRANDS_FILE, desired_order, "BOMOJO_BRANDS")
        elif option == "franchises":
            desired_order = [
                "FRANCHISE",
                "IMDB_ID",
            ]
            load_data(
                db, PRO_BOMOJO_FRANCHISES_FILE, desired_order, "BOMOJO_FRANCHISES"
            )
        elif option == "awards":
            desired_order = [
                "YEAR_FILM",
                "YEAR_CEREMONY",
                "CATEGORY",
                "NOMINEE",
                "MOVIE",
                "WINNER",
                "AWARD",
            ]
            load_data(db, PRO_MOVIES_AWARDS_FILE, desired_order, "MOVIE_AWARDS")
        else:
            logging.error(f"Unknown option: {option}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db.close_connection()
        logging.info("Database connection closed. The script has ended.")


if __name__ == "__main__":
    main()
