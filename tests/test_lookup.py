import pandas as pd

from techchallenge.transform.bronze_to_silver import bronze_to_silver

df = pd.read_parquet("data/bronze/alunos/alunos.parquet")

print("Antes:")
print(df[["rede"]].head())

df = bronze_to_silver(df)

print("\nDepois:")
print(df.head())