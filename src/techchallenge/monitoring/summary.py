class PipelineSummary:

    def __init__(self):

        self.tables = 0
        self.rows = 0
        self.bronze_size = 0
        self.silver_size = 0
        self.total_time = 0

    def add(self, metrics):
        self.tables += 1
        self.rows += metrics["rows"]
        self.bronze_size += metrics["bronze_size"]
        self.silver_size += metrics["silver_size"]
        self.total_time += metrics["duration"]

    def print(self):

        variation = (
            (self.silver_size - self.bronze_size)
            / self.bronze_size
        ) * 100

        print("\n" + "=" * 60)
        print("RESUMO DA EXECUÇÃO")
        print("=" * 60)

        print(f"Tabelas processadas : {self.tables}")
        print(f"Linhas processadas : {self.rows}")
        print(f"Tempo total        : {self.total_time:.2f} s")
        print(f"Camada anterior    : {self.bronze_size:.2f} MB")
        print(f"Camada atual       : {self.silver_size:.2f} MB")
        print(f"Variação           : {variation:.2f}%")

        print("=" * 60)