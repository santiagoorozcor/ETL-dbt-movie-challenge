import os
import logging
import configparser
import pandas as pd
import snowflake.connector


def get_config(config_path: str, config_name: str) -> dict:
    config = configparser.ConfigParser()
    config.read(os.path.join(config_path, config_name))

    return config["snowflake"]


class SnowflakeDatabase:
    def __init__(
        self, config_path: str = "configs", config_name: str = "whse_config.ini"
    ):
        self.config = get_config(config_path, config_name)
        self.conn = self.connection()

    def connection(self):
        try:
            conn = snowflake.connector.connect(
                user=self.config["USER"],
                password=self.config["PASSWORD"],
                account=self.config["ACCOUNT"],
                warehouse=self.config["WAREHOUSE"],
                database=self.config["DATABASE"],
                schema=self.config["SCHEMA"],
            )
            logging.info("Successfully connected to the database.")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to the database: {e}")
            raise

    def switch_database(self, db_name: str):
        self.conn.database = db_name
        logging.info(f"Switched to database: {db_name}")

    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            with self.conn.cursor() as cur:
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
        self.conn.close()
        logging.info("Database connection closed.")
