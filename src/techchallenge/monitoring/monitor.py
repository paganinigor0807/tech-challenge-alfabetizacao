import logging
from datetime import datetime
from pathlib import Path

# Configuração do logger
LOG_PATH = Path("logs")
LOG_PATH.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


class PipelineMonitor:

    def __init__(self, pipeline_name: str, table_name: str):
        self.pipeline_name = pipeline_name
        self.table_name = table_name
    


    def start(self):
        self.start_time = datetime.now()

    def finish(self, input_rows, output_rows, columns, bronze_path, silver_path):

        def get_file_size(path: Path) -> float:
            return round(path.stat().st_size / (1024 * 1024), 2)
        
        end_time = datetime.now()

        duration = end_time - self.start_time
        bronze_size = get_file_size(bronze_path)
        silver_size = get_file_size(silver_path)

        throughput = round(output_rows / duration.total_seconds(),2)
        variation = round(((silver_size - bronze_size) / bronze_size) * 100,2)

        print("\n" + "=" * 60)
        print(f"Pipeline: {self.pipeline_name}")
        print(f"Tabela: {self.table_name}")
        print("=" * 60)

        print("Status: SUCESSO\n")

        print(f"Início : {self.start_time}")
        print(f"Fim    : {end_time}")
        print(f"Tempo  : {duration}")
        print(f"Throughput : {throughput} linhas/s")

        print()

        print(f"Linhas entrada : {input_rows}")
        print(f"Linhas saída   : {output_rows}")
        print(f"Colunas        : {columns}")
        print(f"Camanda anterior : {bronze_size} MB")
        print(f"Camada atual : {silver_size} MB")
        print(f"Variação : {variation}%")



        print("=" * 60)

        logging.info(   
            (
                f"Pipeline={self.pipeline_name} | "
                f"Tabela={self.table_name} | "
                f"Status=SUCESSO | "
                f"Input={input_rows} | "
                f"Output={output_rows} | "
                f"Colunas={columns} | "
                f"Tempo={duration}"
                f"Camada anterior={bronze_size}"
                f"Camada atual={silver_size}"
            )
        )

        return {
            "rows": output_rows,
            "bronze_size": bronze_size,
            "silver_size": silver_size,
            "duration": duration.total_seconds(),
        }

        