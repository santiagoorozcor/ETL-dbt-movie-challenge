import os
import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from typing import List, Optional
from helpers.snowflake_helpers import SnowflakeDatabase
from helpers.web_scraping_helpers import table_to_dataframe, get_href_table
from helpers import (
    RAW_BOMOJO_MOVIES_AREAS_FILE,
    RAW_BOMOJO_MOVIES_REGIONS_FILE,
    RAW_BOMOJO_MOVIES_RELEASES_FILE,
    RAW_BOMOJO_FRANCHISES_FILE,
    RAW_BOMOJO_BRANDS_FILE,
)

load_dotenv()


def fetch_movie_data(url: str) -> Optional[List[BeautifulSoup]]:
    logging.info(f"Fetching data from {url}")
    try:
        req = requests.get(url)
        req.raise_for_status()

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


def scrape_imdb_ids(
    df_data: pd.DataFrame, url_column: str, entity_column: str, key_work: str
) -> pd.DataFrame:
    """
    In the first loop, it retrieves all the links to movies associated with each entity.
    In the second loop, it makes requests to each movie URL to extract the IMDb ID.
    """
    imdb_data = []

    for url, entity_name in zip(df_data[url_column], df_data[entity_column]):
        logging.info("Processing: %s", entity_name)

        total_movies = 0
        total_movies_found = 0

        try:
            req = requests.get(url)
            soup = BeautifulSoup(req.text, "html.parser")

            df_movies = get_href_table(soup, key_work)
            df_movies["href"] = df_movies["href"].apply(
                lambda href: f"https://www.boxofficemojo.com{href}"
            )
            total_movies += len(df_movies)

            for movie_url in df_movies["href"]:
                logging.info("Scraping movie URL: %s", movie_url)

                try:
                    req_movie = requests.get(movie_url)
                    req_movie.raise_for_status()

                    soup_movie = BeautifulSoup(req_movie.text, "html.parser")
                    first_pro_imdb_link = soup_movie.find(
                        "a",
                        href=lambda href: href and "https://pro.imdb.com/title" in href,
                    )

                    if first_pro_imdb_link:
                        imdb_id = (
                            str(first_pro_imdb_link).split("/title/")[-1].split("/")[0]
                        )
                        total_movies_found += 1
                    else:
                        imdb_id = None

                    imdb_data.append({"Entity": entity_name, "IMDB_ID": imdb_id})

                except requests.exceptions.RequestException as e:
                    logging.error(
                        "Error occurred while scraping movie URL %s: %s",
                        movie_url,
                        str(e),
                    )
                    continue

            logging.info("Total movies found: %d/%d", total_movies_found, total_movies)

        except requests.exceptions.RequestException as e:
            logging.error("Error occurred while processing URL %s: %s", url, str(e))
            continue

    return pd.DataFrame(imdb_data)


def append_to_csv(df: pd.DataFrame, csv_file: str) -> None:
    try:
        if not Path(csv_file).exists():
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
            append_to_csv(df_movie_releases, RAW_BOMOJO_MOVIES_RELEASES_FILE)

            table_start_index = 1
            csv_file_name = RAW_BOMOJO_MOVIES_REGIONS_FILE
        else:
            table_start_index = 0
            csv_file_name = RAW_BOMOJO_MOVIES_AREAS_FILE

        if len(tables) > 1:
            for table in tables[table_start_index:]:
                df_movie_areas = table_to_dataframe(table)
                df_movie_areas["IMDB_ID"] = imdb_id
                append_to_csv(df_movie_areas, csv_file_name)

        remaining_ids = total_ids - index
        logging.info(f"Processed IMDb ID {imdb_id}. Remaining IDs: {remaining_ids}")


def get_franchises() -> None:
    url = "https://www.boxofficemojo.com/franchise/?ref_=bo_nb_gs_secondarytab"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")

    df_franchises = get_href_table(soup, "franchise/fr")
    df_franchises["href"] = df_franchises["href"].apply(
        lambda href: f"https://www.boxofficemojo.com{href}"
    )

    df_franchises_imdb_id = scrape_imdb_ids(df_franchises, "href", "Name", "release/rl")

    append_to_csv(df_franchises_imdb_id, RAW_BOMOJO_FRANCHISES_FILE)


def get_brands() -> None:
    url = "https://www.boxofficemojo.com/brand/?ref_=bo_nb_frs_secondarytab"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")

    df_brands = get_href_table(soup, "brand/bn")
    df_brands["href"] = df_brands["href"].apply(
        lambda href: f"https://www.boxofficemojo.com{href}"
    )
    df_brands

    df_brands_imdb_id = scrape_imdb_ids(df_brands, "href", "Name", "release/rl")

    append_to_csv(df_brands_imdb_id, RAW_BOMOJO_BRANDS_FILE)


def main():
    if len(sys.argv) < 2:
        print("Usage: script.py <option>")
        sys.exit(1)

    option = sys.argv[1]

    # Set up logging
    logging.basicConfig(
        filename=f"scraper_{option}.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    logging.info("Script started with option: %s", option)

    try:
        if option == "countries":
            try:
                db = SnowflakeDatabase(
                    os.getenv("USER"),
                    os.getenv("PASSWORD"),
                    os.getenv("ACCOUNT"),
                    os.getenv("WAREHOUSE"),
                    os.getenv("DATABASE"),
                    os.getenv("SCHEMA"),
                )

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

                get_countries(df_imdb_id)

            except Exception as e:
                logging.error(f"An error occurred: {e}")
            finally:
                db.close_connection()
                logging.info("Database connection closed. The script has ended.")

        elif option == "franchises":
            get_franchises()
        elif option == "brands":
            get_brands()
        else:
            logging.error(f"Unknown option: {option}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("The process has been completed.")


if __name__ == "__main__":
    main()
