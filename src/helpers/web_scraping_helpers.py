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
