from techchallenge.config.mapping_loader import load_mapping

uf = load_mapping("uf.csv")

print(uf.head())
print(uf.columns)