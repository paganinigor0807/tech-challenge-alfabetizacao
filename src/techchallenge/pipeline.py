from pathlib import Path
import pandas as pd
import logging
from techchallenge.config.bronze_tables import BRONZE_TABLES
from techchallenge.extract.services.extraction_service import ExtractionService
from techchallenge.load.parquet_writer import ParquetWriter
from techchallenge.transform.bronze_to_silver import bronze_to_silver
from techchallenge.monitoring.monitor import PipelineMonitor
from techchallenge.monitoring.summary import PipelineSummary
import shutil


class BronzePipeline:

    def __init__(self):
        self.extract_service = ExtractionService()
        self.writer = ParquetWriter()

    def run(self, limit: int | None = None):
        summary = PipelineSummary()

        for table in BRONZE_TABLES:
            try:

                monitor = PipelineMonitor(
                pipeline_name="Bronze",
                table_name=table
                )

                monitor.start()
            
                print(f"Extraindo {table}...")

                bronze_path = Path(
                    f"data/bronze/batch/{table}/{table}.parquet"
                )

                df = self.extract_service.extract_table(
                    table_name=table,
                    limit=limit
                )

                self.writer.save(
                    dataframe=df,
                    output_path=Path(bronze_path),
                )

                metrics = monitor.finish(
                    input_rows=len(df),
                    output_rows=len(df),
                    columns=len(df.columns),
                    bronze_path=bronze_path,
                    silver_path=bronze_path
                )

                print(f"{table} concluída.")
                summary.add(metrics)
        
            except Exception as e:
                logging.exception(f"Erro ao processar {table}")
                print(f"Erro na tabela {table} na camada Bronze: {e}")

        summary.print()


class SilverPipeline:

    def __init__(self):
        self.writer = ParquetWriter()

    def load_bronze_table(self, table):

        batch_path = Path(
            f"data/bronze/batch/{table}/{table}.parquet"
        )

        dataframe = pd.read_parquet(batch_path)

        # Apenas a tabela alunos possui streaming
        if table != "alunos":
            return dataframe

        streaming_path = Path(
            "data/bronze/streaming/alunos"
        )

        if streaming_path.exists():

            streaming_files = list(
                streaming_path.glob("*.parquet")
            )

            if streaming_files:

                streaming_df = pd.concat(
                    [pd.read_parquet(file) for file in streaming_files],
                    ignore_index=True
                )

                dataframe = pd.concat(
                    [dataframe, streaming_df],
                    ignore_index=True
                )

        return dataframe

    def archive_streaming_files(self):

        streaming_path = Path(
            "data/bronze/streaming/alunos"
        )

        processed_path = Path(
            "data/bronze/streaming/processed"
        )

        processed_path.mkdir(
            parents=True,
            exist_ok=True
        )

        for file in streaming_path.glob("*.parquet"):

            shutil.move(
                file,
                processed_path / file.name
            )

    def run(self):

        summary = PipelineSummary()

        for table in BRONZE_TABLES:

            try:

                monitor = PipelineMonitor(
                    pipeline_name="Silver",
                    table_name=table
                )

                monitor.start()

                print(f"Transformando {table}...")

                bronze_path = Path(
                    f"data/bronze/batch/{table}/{table}.parquet"
                )

                # Toda a lógica de leitura fica aqui
                dataframe = self.load_bronze_table(table)

                input_rows = len(dataframe)

                print(
                    f"{table}: {input_rows:,} registros carregados"
                )

                dataframe = bronze_to_silver(
                    dataframe,
                    table
                )

                silver_path = Path(
                    f"data/silver/{table}/{table}.parquet"
                )

                self.writer.save(
                    dataframe=dataframe,
                    output_path=silver_path
                )

                if table == "alunos":
                    self.archive_streaming_files()

                metrics = monitor.finish(
                    input_rows=input_rows,
                    output_rows=len(dataframe),
                    columns=len(dataframe.columns),
                    bronze_path=bronze_path,
                    silver_path=silver_path
                )

                summary.add(metrics)

                print(f"{table} transformação concluída.")

            except Exception as e:

                logging.exception(f"Erro ao processar {table}")

                print(
                    f"Erro na tabela {table} na camada Silver: {e}"
                )

        summary.print()


class GoldPipeline:

    def __init__(self):
        self.writer = ParquetWriter()

    def load_silver(self):

        return pd.read_parquet(
            Path("data/silver/alunos/alunos.parquet")
        )

    # ===========================
    # DIMENSÕES
    # ===========================
    def load_metas(self):

        self.meta_municipio = pd.read_parquet(
            Path(
                "data/silver/meta_alfabetizacao_municipio/meta_alfabetizacao_municipio.parquet"
            )
        )

        self.meta_uf = pd.read_parquet(
            Path(
                "data/silver/meta_alfabetizacao_uf/meta_alfabetizacao_uf.parquet"
            )
        )

        self.meta_brasil = pd.read_parquet(
            Path(
                "data/silver/meta_alfabetizacao_brasil/meta_alfabetizacao_brasil.parquet"
            )
        )


    def add_meta(self, dataframe):

        dataframe["meta"] = dataframe.apply(
            lambda row: row.get(
                f"meta_alfabetizacao_{int(row['ano'])}",
                None
            ),
            axis=1
        )

        colunas_meta = [
            coluna
            for coluna in dataframe.columns
            if coluna.startswith("meta_alfabetizacao_")
        ]

        dataframe = dataframe.drop(columns=colunas_meta)

        if "sigla_uf" in dataframe.columns:
            dataframe = dataframe.drop(columns=["sigla_uf"])
        
        return dataframe


    def build_dimensions(self, dataframe):

        # ===========================
        # DIMENSÃO MUNICÍPIO
        # ===========================
        self.dim_municipio = (
            dataframe[
                ["id_municipio", "Municipio", "UF"]
            ]
            .drop_duplicates()
            .sort_values("Municipio")
            .reset_index(drop=True)
        )

        # ===========================
        # DIMENSÃO REDE
        # ===========================
        self.dim_rede = (
            dataframe[
                ["ID_Rede", "Rede"]
            ]
            .drop_duplicates()
            .sort_values("ID_Rede")
            .reset_index(drop=True)
        )

        # ===========================
        # DIMENSÃO TEMPO
        # ===========================
        self.dim_tempo = (
            dataframe[
                ["ano"]
            ]
            .drop_duplicates()
            .sort_values("ano")
            .reset_index(drop=True)
        )

        # ===========================
        # DIMENSÃO UF
        # ===========================
        self.dim_uf = (
            dataframe[
                ["UF"]
            ]
            .drop_duplicates()
            .sort_values("UF")
            .reset_index(drop=True)
        )

        print("Dimensões criadas.")

    # ===========================
    # FATOS
    # ===========================

    def calculate_metrics(self, dataframe):

        dataframe["taxa_alfabetizacao"] = (
            dataframe["alunos_alfabetizados"]
            / dataframe["alunos_avaliados"]
            * 100
        ).round(2)

        dataframe["taxa_presenca"] = (
            dataframe["alunos_presentes"]
            / dataframe["alunos_avaliados"]
            * 100
        ).round(2)

        dataframe["taxa_preenchimento"] = (
            dataframe["provas_preenchidas"]
            / dataframe["alunos_avaliados"]
            * 100
        ).round(2)

        dataframe["meta"] = None

        return dataframe


    def build_facts(self, dataframe):

        # ==================================================
        # FATO ALFABETIZAÇÃO MUNICÍPIO
        # ==================================================

        self.fato_municipio = (
            dataframe
            .groupby(
                [
                    "ano",
                    "id_municipio",
                    "UF",
                    "ID_Rede",
                    "Rede"
                ],
                as_index=False
            )
            .agg(
                alunos_avaliados=("id_aluno", "count"),

                alunos_alfabetizados=(
                    "alfabetizado",
                    lambda x: (x == "Aluno alfabetizado").sum()
                ),

                alunos_presentes=(
                    "presenca",
                    lambda x: (x == "Presente").sum()
                ),

                provas_preenchidas=(
                    "preenchimento_caderno",
                    lambda x: (x == "Prova preenchida").sum()
                )
            )
        )

        self.fato_municipio = self.calculate_metrics(
            self.fato_municipio
        )

        self.fato_municipio = self.fato_municipio.merge(
            self.meta_municipio[
                [
                    "ano",
                    "id_municipio",
                    "ID_Rede",
                    "meta_alfabetizacao_2024",
                    "meta_alfabetizacao_2025",
                    "meta_alfabetizacao_2026",
                    "meta_alfabetizacao_2027",
                    "meta_alfabetizacao_2028",
                    "meta_alfabetizacao_2029",
                    "meta_alfabetizacao_2030"
                ]
            ],
            on=["ano", "id_municipio", "ID_Rede"],
            how="left"
        )

        self.fato_municipio = self.add_meta(self.fato_municipio)


        # ==================================================
        # FATO ALFABETIZAÇÃO UF
        # ==================================================

        self.fato_uf = (
            self.fato_municipio
            .groupby(
                [
                    "ano",
                    "UF",
                    "ID_Rede"
                ],
                as_index=False
            )
            .agg(
                alunos_avaliados=("alunos_avaliados", "sum"),

                alunos_alfabetizados=("alunos_alfabetizados", "sum"),

                alunos_presentes=("alunos_presentes", "sum"),

                provas_preenchidas=("provas_preenchidas", "sum")
            )
        )

        self.fato_uf = self.calculate_metrics(
            self.fato_uf
        )

        self.fato_uf = self.fato_uf.merge(
            self.meta_uf[
                [
                    "ano",
                    "UF",
                    "ID_Rede",
                    "meta_alfabetizacao_2024",
                    "meta_alfabetizacao_2025",
                    "meta_alfabetizacao_2026",
                    "meta_alfabetizacao_2027",
                    "meta_alfabetizacao_2028",
                    "meta_alfabetizacao_2029",
                    "meta_alfabetizacao_2030"
                ]
            ],
            on=["ano", "UF", "ID_Rede"],
            how="left"
        )

        self.fato_uf = self.add_meta(self.fato_uf)

        # ==================================================
        # FATO ALFABETIZAÇÃO BRASIL
        # ==================================================

        self.fato_brasil = (
            self.fato_uf
            .groupby(
                [
                    "ano",
                    "ID_Rede"
                ],
                as_index=False
            )
            .agg(
                alunos_avaliados=("alunos_avaliados", "sum"),

                alunos_alfabetizados=("alunos_alfabetizados", "sum"),

                alunos_presentes=("alunos_presentes", "sum"),

                provas_preenchidas=("provas_preenchidas", "sum")
            )
        )

        self.fato_brasil = self.calculate_metrics(
            self.fato_brasil
        )

        self.fato_brasil = self.fato_brasil.merge(
            self.meta_brasil[
                [
                    "ano",
                    "ID_Rede",
                    "meta_alfabetizacao_2024",
                    "meta_alfabetizacao_2025",
                    "meta_alfabetizacao_2026",
                    "meta_alfabetizacao_2027",
                    "meta_alfabetizacao_2028",
                    "meta_alfabetizacao_2029",
                    "meta_alfabetizacao_2030"
                ]
            ],
            on=["ano", "ID_Rede"],
            how="left"
        )

        self.fato_brasil = self.add_meta(self.fato_brasil)

        # ==================================================
        # REMOVER COLUNAS DESCRITIVAS
        # ==================================================

        self.fato_municipio = self.fato_municipio[
            [
                "ano",
                "id_municipio",
                "ID_Rede",
                "alunos_avaliados",
                "alunos_alfabetizados",
                "alunos_presentes",
                "provas_preenchidas",
                "taxa_alfabetizacao",
                "taxa_presenca",
                "taxa_preenchimento",
                "meta"
            ]
        ]

        self.fato_uf = self.fato_uf[
            [
                "ano",
                "UF",
                "ID_Rede",
                "alunos_avaliados",
                "alunos_alfabetizados",
                "alunos_presentes",
                "provas_preenchidas",
                "taxa_alfabetizacao",
                "taxa_presenca",
                "taxa_preenchimento",
                "meta"
            ]
        ]

        self.fato_brasil = self.fato_brasil[
            [
                "ano",
                "ID_Rede",
                "alunos_avaliados",
                "alunos_alfabetizados",
                "alunos_presentes",
                "provas_preenchidas",
                "taxa_alfabetizacao",
                "taxa_presenca",
                "taxa_preenchimento",
                "meta"
            ]
        ]

        print("Tabelas fato criadas.")

    def save_dimensions(self):

        self.writer.save(
            dataframe=self.dim_municipio,
            output_path=Path(
                "data/gold/dimensions/dim_municipio/dim_municipio.parquet"
            )
        )

        self.writer.save(
            dataframe=self.dim_rede,
            output_path=Path(
                "data/gold/dimensions/dim_rede/dim_rede.parquet"
            )
        )

        self.writer.save(
            dataframe=self.dim_tempo,
            output_path=Path(
                "data/gold/dimensions/dim_tempo/dim_tempo.parquet"
            )
        )

        self.writer.save(
            dataframe=self.dim_uf,
            output_path=Path(
                "data/gold/dimensions/dim_uf/dim_uf.parquet"
            )
        )

        print("Dimensões salvas.")


    def save_facts(self):

        self.writer.save(
            dataframe=self.fato_municipio,
            output_path=Path(
                "data/gold/facts/fato_alfabetizacao_municipio/fato_alfabetizacao_municipio.parquet"
            )
        )

        self.writer.save(
            dataframe=self.fato_uf,
            output_path=Path(
                "data/gold/facts/fato_alfabetizacao_uf/fato_alfabetizacao_uf.parquet"
            )
        )

        self.writer.save(
            dataframe=self.fato_brasil,
            output_path=Path(
                "data/gold/facts/fato_alfabetizacao_brasil/fato_alfabetizacao_brasil.parquet"
            )
        )

        print("Tabelas fato salvas.")

    
    def run(self):

        summary = PipelineSummary()

        monitor = PipelineMonitor(
            pipeline_name="Gold",
            table_name="Indicadores"
        )

        monitor.start()

        dataframe = self.load_silver()

        self.load_metas()

        self.build_dimensions(dataframe)

        self.build_facts(dataframe)

        self.save_dimensions()

        self.save_facts()

        metrics = monitor.finish(
            input_rows=len(dataframe),
            output_rows=len(self.fato_municipio),
            columns=len(self.fato_municipio.columns),
            bronze_path=Path("data/silver/alunos/alunos.parquet"),
            silver_path=Path(
                "data/gold/facts/fato_alfabetizacao_municipio/fato_alfabetizacao_municipio.parquet"
            )
        )

        summary.add(metrics)
        summary.print()

        print("Gold concluída.")