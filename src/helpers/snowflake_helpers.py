import os
import logging
import pandas as pd
import snowflake.connector
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class SnowflakeDatabase:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str
    conn: any = field(init=False)

    def __post_init__(self):
        self.conn = self._connect()

    def _connect(self):
        try:
            conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
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
