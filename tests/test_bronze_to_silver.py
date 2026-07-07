import pandas as pd
from techchallenge.transform.bronze_to_silver import bronze_to_silver

bronze = pd.read_parquet("data/bronze/alunos/alunos.parquet")

silver = bronze_to_silver(bronze)

print("Bronze:", bronze.shape)
print("Silver:", silver.shape)

print()

print(bronze.dtypes)

print()

print(silver.dtypes)