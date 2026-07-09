import pandas as pd

bronze = pd.read_parquet(
    "data/bronze/batch/alunos/alunos.parquet"
)

silver = pd.read_parquet(
    "data/silver/alunos/alunos.parquet"
)

print(bronze.count())
print(silver.count())
