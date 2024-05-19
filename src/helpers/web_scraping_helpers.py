import pandas as pd
from bs4 import Tag
from typing import List


def table_to_dataframe(table: List[Tag], header_row_index: int = 0) -> pd.DataFrame:
    data = []
    for row in table:
        cols = row.find_all(["th", "td"])
        cols = [col.get_text(strip=True) for col in cols]
        data.append(cols)

    header = data[header_row_index]
    data = data[header_row_index + 1 :]

    return pd.DataFrame(data, columns=header)


def get_href_table(soup: List[Tag], key_word: str) -> pd.DataFrame:
    data = []

    for link in soup.find_all("a", {"class": "a-link-normal"}):
        href = link.get("href")
        name = link.text.strip()
        if key_word in href:
            data.append((href, name))

    return pd.DataFrame(data, columns=["href", "Name"])
