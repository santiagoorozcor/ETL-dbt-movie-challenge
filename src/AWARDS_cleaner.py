import logging
import pandas as pd
from helpers import RAW_OSCARS_FILE, RAW_RAZZIES_FILE, PRO_MOVIES_AWARDS_FILE


def clean_oscars(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"name": "Nominee", "film": "Movie"})
    df.columns = df.columns.str.upper()
    df["MOVIE"] = df["MOVIE"].str.replace(";", "")
    df["AWARD"] = "OSCAR"

    return df


def clean_razzies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"Year": "YEAR_CEREMONY"})
    df.columns = df.columns.str.upper()
    df["WINNER"] = df["WINNER"].str.strip().str.lower().map({'true': True, 'false': False})
    df["YEAR_FILM"] = df["YEAR_CEREMONY"] - 1
    df["AWARD"] = "RAZZIES"

    return df


def process_awards(df_oscars: pd.DataFrame, df_razzies: pd.DataFrame) -> None:
    df_awards = pd.concat([df_oscars, df_razzies], axis=0)
    logging.info("Data processing complete. Saving to file...")

    df_awards["WINNER"] = df_awards["WINNER"].astype(bool)
    df_awards.to_parquet(PRO_MOVIES_AWARDS_FILE, index=False)
    logging.info("Data successfully processed and saved to %s", PRO_MOVIES_AWARDS_FILE)


def main():
    # Set up logging
    logging.basicConfig(
        filename="cleaner_awards.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    logging.info("Script started")

    try:
        logging.info("Reading Oscars data...")
        df_oscars = pd.read_csv(
            RAW_OSCARS_FILE,
            encoding="utf-8",
            usecols=[
                "year_film",
                "year_ceremony",
                "category",
                "name",
                "film",
                "winner",
            ],
        )
        df_oscars = clean_oscars(df_oscars)
        logging.info("Oscars data cleaned.")

        logging.info("Reading Razzies data...")
        df_razzies = pd.read_csv(RAW_RAZZIES_FILE, encoding="utf-8")
        df_razzies = clean_razzies(df_razzies)
        logging.info("Razzies data cleaned.")

        process_awards(df_oscars, df_razzies)
    except Exception as e:
        logging.exception(f"An error occurred during the script execution: {e}")


if __name__ == "__main__":
    main()
