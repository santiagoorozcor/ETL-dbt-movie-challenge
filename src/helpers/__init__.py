from pathlib import Path


def get_data_dir(data_type: str) -> Path:
    # Define the relative path from the script
    relative_path = Path("data") / data_type

    # Try to find the data directory in the current location or in the parent directories
    current_path = Path(__file__).resolve().parent
    for _ in range(3):  # Limit the search to three parent levels
        potential_path = current_path / relative_path
        if potential_path.exists():
            return potential_path
        current_path = current_path.parent

    raise FileNotFoundError("The data directory could not be found.")


# Data directories
RAW_DATA_DIR = get_data_dir("raw")
PROCESSED_DATA_DIR = get_data_dir("processed")
MAPPING_DATA_DIR = get_data_dir("mappings")


# Paths to specific files
RAW_BOMOJO_MOVIES_RELEASES_FILE = RAW_DATA_DIR / "BOMOJO_MOVIES_RELEASES.csv"
RAW_BOMOJO_MOVIES_REGIONS_FILE = RAW_DATA_DIR / "BOMOJO_MOVIES_REGIONS.csv"
RAW_BOMOJO_MOVIES_AREAS_FILE = RAW_DATA_DIR / "BOMOJO_MOVIES_AREAS.csv"
RAW_BOMOJO_FRANCHISES_FILE = RAW_DATA_DIR / "BOMOJO_FRANCHISES.csv"
RAW_BOMOJO_BRANDS_FILE = RAW_DATA_DIR / "BOMOJO_BRANDS.csv"
RAW_OSCARS_FILE = RAW_DATA_DIR / "the_oscar_award.csv"
RAW_RAZZIES_FILE = RAW_DATA_DIR / "razzies.csv"

PRO_BOMOJO_COUNTRIES_FILE = PROCESSED_DATA_DIR / "BOMOJO_MOVIES_COUNTRIES.parquet"
PRO_BOMOJO_RELEASES_FILE = PROCESSED_DATA_DIR / "BOMOJO_MOVIES_RELEASES.parquet"
PRO_BOMOJO_FRANCHISES_FILE = PROCESSED_DATA_DIR / "BOMOJO_FRANCHISES.parquet"
PRO_BOMOJO_BRANDS_FILE = PROCESSED_DATA_DIR / "BOMOJO_BRANDS.parquet"
PRO_MOVIES_AWARDS_FILE = PROCESSED_DATA_DIR / "MOVIES_AWARDS.parquet"

COUNTRY_REGION_MAPPINGS = MAPPING_DATA_DIR / "country_and_region_mappings.json"
