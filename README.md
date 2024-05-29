# ETL dbt movie challenge

This repository was developed to execute the ETL (Extract, Transform, Load) process for a challenge organized by Paradime. The challenge details can be found here: [Paradime Data Modeling Challenge](https://www.paradime.io/dbt-data-modeling-challenge-movie-edition#div-registration-form). The ETL pipeline includes web scraping as part of the extraction phase, followed by data cleaning and loading, which is configured by default for Snowflake.

> [!NOTE]
> The `robots.txt` file for Box Office Mojo has been validated, and it allows web scraping. You can view the `robots.txt` file [here](https://www.boxofficemojo.com/robots.txt).

## Installation

Follow these steps to set up the project:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/santiagoorozcor/ETL-dbt-movie-challenge.git
    cd <your_project>
    ```

2. **Install Poetry:**

   Follow the instructions on the [Poetry documentation](https://python-poetry.org/docs/). It is recommended to install Poetry using `pipx`

> [!NOTE]
> Using `pipx` to install Poetry ensures it is isolated from your other Python packages and environments.

3. **Install the project dependencies:**

    Install the dependencies with the following command:

    ```bash
    poetry install
    ```

4. **Activate the virtual environment:**

    Run `poetry shell` to activate the virtual environment for your project.

    ```bash
    poetry shell
    ```

Your environment should now be set up and ready for use.

## Usage

### Extraction (E)

To run the extraction phase, use the following command from the root of the project:

```bash
poetry run python ./src/BOMOJO_scraper.py <option>
```

Options:
- countries
- franchises
- brands

> [!CAUTION]
> For the countries option, the IDs are fetched from Snowflake. You need to modify the code accordingly to ensure it connects to your Snowflake instance.

All extracted files will be saved as CSV files in the `data/raw` directory.

### Transformation (T)

#### Cleaning Scraper Data

To clean the data extracted by the scrapers, use the following command from the root of the project:

```bash
poetry run python ./src/BOMOJO_cleaner.py <option>
```

| Option       | Description                                                |
|--------------|------------------------------------------------------------|
| `countries`  | Cleans data about countries. Requires the `countries` scraper output. |
| `releases`   | Cleans data about releases, related to the `countries` scraper output. |
| `franchises` | Cleans data about franchises.                              |
| `brands`     | Cleans data about brands.                                  |

#### Cleaning Awards Data
The awards data was sourced from Kaggle and placed in the data/raw directory. The sources are:

- [The Oscar Award Dataset](https://www.kaggle.com/datasets/unanimad/the-oscar-award/data)
- [Golden Raspberry Awards Dataset](https://www.kaggle.com/datasets/martj42/golden-raspberry-awards)

To clean the awards data, use the following command from the root of the project:

```bash
poetry run python ./src/AWARDS_cleaner.py
```
No additional options are required for this command.

> [!NOTE]
> All cleaned files will be saved as Parquet files in the `data/processed` directory.

### Load (L)

To load the cleaned data into Snowflake, use the following command from the root of the project:

```bash
poetry run python ./src/DATA_loader.py <option>
```

| Option       | Description                            |
|--------------|----------------------------------------|
| `releases`   | Loads release data into Snowflake.     |
| `countries`  | Loads country data into Snowflake.     |
| `brands`     | Loads brand data into Snowflake.       |
| `franchises` | Loads franchise data into Snowflake.   |
| `awards`     | Loads awards data into Snowflake.      |
