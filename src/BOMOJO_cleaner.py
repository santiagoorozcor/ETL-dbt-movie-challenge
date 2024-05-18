import sys
import json
import logging
import pandas as pd
from helpers import (
    RAW_BOMOJO_MOVIES_AREAS_FILE,
    RAW_BOMOJO_MOVIES_REGIONS_FILE,
    RAW_BOMOJO_MOVIES_RELEASES_FILE,
    PRO_BOMOJO_COUNTRIES_FILE,
    COUNTRY_REGION_MAPPINGS,
)


def load_mappings(mappings_file: str) -> tuple:
    with open(mappings_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    country_mapping = mappings.get("country_mapping", {})
    market_regions = mappings.get("market_regions", {})
    return country_mapping, market_regions


def normalize_country_names(
    df: pd.DataFrame, column_name: str, country_mapping: dict
) -> pd.DataFrame:
    df[column_name] = df[column_name].replace(country_mapping)
    df[column_name] = df[column_name].apply(
        lambda x: "Turkey" if x.endswith("kiye") else x
    )
    df[column_name] = df[column_name].apply(
        lambda x: "Curaçao" if x.endswith("ao") else x
    )
    return df


def classify_region(
    df: pd.DataFrame, column_name: str, market_regions: dict
) -> pd.DataFrame:
    # Create a lookup dictionary for faster region assignment
    region_lookup = {
        country: region
        for region, countries in market_regions.items()
        for country in countries
    }
    df["REGION"] = df[column_name].apply(lambda x: region_lookup.get(x, "OTHER"))
    return df


def process_countries(country_mapping: dict, market_regions: dict):
    try:
        df_areas = pd.read_csv(
            RAW_BOMOJO_MOVIES_AREAS_FILE,
            encoding="utf-8",
            usecols=["Area", "Gross", "IMDB_ID"],
        ).rename(columns={"Area": "AREA", "Gross": "LIFETIME_GROSS"})

        df_areas.dropna(subset=["LIFETIME_GROSS"], inplace=True)

        df_regions = pd.read_csv(
            RAW_BOMOJO_MOVIES_REGIONS_FILE,
            encoding="utf-8",
            usecols=["APAC", "# Releases", "Lifetime Gross", "IMDB_ID"],
        ).rename(
            columns={
                "APAC": "AREA",
                "# Releases": "RELEASES",
                "Lifetime Gross": "LIFETIME_GROSS",
            }
        )

        logging.info("Data loaded successfully.")

        df_countries = pd.concat([df_areas, df_regions], ignore_index=True)
        df_countries["RELEASES"] = df_countries["RELEASES"].fillna(1).astype(int)

        df_countries = normalize_country_names(df_countries, "AREA", country_mapping)
        df_countries = classify_region(df_countries, "AREA", market_regions)

        df_countries["LIFETIME_GROSS"] = (
            df_countries["LIFETIME_GROSS"]
            .str.replace("[\$,]", "", regex=True)
            .astype(int)
        )

        logging.info("Data processing complete. Saving to file...")

        df_countries.to_parquet(PRO_BOMOJO_COUNTRIES_FILE, index=False)
        logging.info(
            "Data successfully processed and saved to %s", PRO_BOMOJO_COUNTRIES_FILE
        )

    except Exception as e:
        logging.error("An error occurred during data processing: %s", str(e))


def main():
    if len(sys.argv) < 2:
        print("Usage: script.py <option>")
        sys.exit(1)

    option = sys.argv[1]

    # Set up logging
    logging.basicConfig(
        filename=f"cleaner_{option}.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    logging.info("Script started with option: %s", option)

    try:
        country_mapping, market_regions = load_mappings(COUNTRY_REGION_MAPPINGS)

        if option == "countries":
            process_countries(country_mapping, market_regions)
        else:
            logging.error("Invalid option provided: %s", option)
            sys.exit(1)
    except Exception as e:
        logging.exception("An error occurred during the script execution.")
        sys.exit(1)


if __name__ == "__main__":
    main()
