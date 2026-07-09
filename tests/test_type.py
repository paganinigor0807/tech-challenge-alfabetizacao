from pathlib import Path
import pandas as pd

df = pd.read_parquet(
    Path("data/silver/meta_alfabetizacao_brasil/meta_alfabetizacao_brasil.parquet")
)

print(df.columns)