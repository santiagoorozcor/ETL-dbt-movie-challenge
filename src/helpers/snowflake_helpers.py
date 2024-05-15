import os
import logging
import configparser
import pandas as pd
import snowflake.connector
from typing import Optional, Dict
from contextlib import contextmanager


def get_config(config_path: str, config_name: str) -> Dict[str, str]:
    config = configparser.ConfigParser()
    config.read(os.path.join(config_path, config_name))

    if "snowflake" not in config:
        logging.error("Configuration section 'snowflake' not found in config file.")
        raise ValueError("Configuration section 'snowflake' not found in config file.")

    return config["snowflake"]


class SnowflakeDatabase:
    def __init__(
        self, config_path: str = "configs", config_name: str = "whse_config.ini"
    ):
        self.config = get_config(config_path, config_name)
        self.conn = self._connect()

    def _connect(self):
        try:
            conn = snowflake.connector.connect(
                user=self.config["USER"],
                password=self.config["PASSWORD"],
                account=self.config["ACCOUNT"],
                warehouse=self.config["WAREHOUSE"],
                database=self.config["DATABASE"],
                schema=self.config["SCHEMA"],
            )
            logging.info("Successfully connected to the Snowflake database.")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to the Snowflake database: {e}")
            raise

    def switch_database(self, db_name: str) -> None:
        try:
            self.conn.cursor().execute(f"USE DATABASE {db_name}")
            logging.info(f"Switched to database: {db_name}")
        except Exception as e:
            logging.error(f"Failed to switch database to {db_name}: {e}")
            raise

    @contextmanager
    def managed_cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            with self.managed_cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=column_names)
            logging.info("Query executed successfully.")
            return df
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise

    def close_connection(self):
        try:
            self.conn.close()
            logging.info("Database connection closed.")
        except Exception as e:
            logging.error(f"Failed to close the database connection: {e}")
