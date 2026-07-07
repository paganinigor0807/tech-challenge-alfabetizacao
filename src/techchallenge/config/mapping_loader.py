from pathlib import Path

import pandas as pd


BASE_PATH = Path(__file__).parent / "mappings"


def load_mapping(file_name: str) -> pd.DataFrame:
    return pd.read_csv(BASE_PATH / file_name)