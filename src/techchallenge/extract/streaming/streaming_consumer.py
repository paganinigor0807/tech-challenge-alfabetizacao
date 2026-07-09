from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import shutil

class StreamingConsumer:

    def __init__(self):

        self.input_path = Path("data/temp_streaming_input")

        self.output_path = Path("data/bronze/streaming/alunos")

        self.processed_path = Path("data/temp_streaming_processed")

    def _find_json_files(self):

        return sorted(self.input_path.glob("*.json"))
    

    def _load_json_files(self, files):

        registros = []

        for file in files:

            with open(file, "r", encoding="utf-8") as f:
                registros.append(json.load(f))

        return pd.DataFrame(registros)
    
    def _save_parquet(self, dataframe):

        self.output_path.mkdir(parents=True, exist_ok=True)

        file_name = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_file = self.output_path / f"streaming_{file_name}.parquet"

        dataframe.to_parquet(
            output_file,
            index=False
        )

        print(f"Arquivo salvo em: {output_file}")
    
    def _move_processed_files(self, files):

        self.processed_path.mkdir(
            parents=True,
            exist_ok=True
        )

        for file in files:

            destination = self.processed_path / file.name

            shutil.move(file, destination)

    def run(self):

        print("Streaming Consumer iniciado.")

        arquivos = self._find_json_files()

        print(f"Arquivos encontrados: {len(arquivos)}")

        if not arquivos:
            print("Nenhum arquivo encontrado.")
            return

        dataframe = self._load_json_files(arquivos)
        self._save_parquet(dataframe)
        self._move_processed_files(arquivos)

        print("\nColunas:")
    
