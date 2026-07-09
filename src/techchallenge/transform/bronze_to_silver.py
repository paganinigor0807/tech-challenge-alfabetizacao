import pandas as pd
from techchallenge.transform.cleaning import strip_string_columns
from techchallenge.transform.standardization import standardize_column_names
from techchallenge.transform.validation import remove_duplicates
from techchallenge.transform.lookup import enrich_dataframe
from techchallenge.config.mapping_loader import load_mapping


def bronze_to_silver(df, table_name):

    df = standardize_column_names(df)

    df = strip_string_columns(df)

    df = remove_duplicates(df)


#Enriquecimento das tabelas com a tradução dos códigos
    if "rede" in df.columns:

        # Tabelas de metas
        if table_name in [
            "meta_alfabetizacao_municipio",
            "meta_alfabetizacao_uf",
            "meta_alfabetizacao_brasil"
        ]:

            df = df.rename(columns={"rede": "Rede"})

            rede_map = pd.read_csv("src/techchallenge/config/mappings/rede.csv")

            df = df.merge(
                rede_map,
                on="Rede",
                how="left"
            )

            df = df.rename(
                columns={
                    "ID_tipo_rde": "ID_Rede"
                }
            )

        # Demais tabelas
        else:

            df = enrich_dataframe(
                df,
                mapping_file="rede.csv",
                left_on="rede",
                right_on="ID_tipo_rde",
                drop_columns=["rede"]
            )

            df = df.rename(
                columns={
                    "ID_tipo_rde": "ID_Rede"
                }
            )
            
    if "ID_Rede" in df.columns:
        df["ID_Rede"] = (
            pd.to_numeric(
                df["ID_Rede"],
                errors="coerce"
            )
            .astype("Int64")
        )
    
    if "presenca" in df.columns:
        df = enrich_dataframe(
            df=df,
            mapping_file="presenca.csv",
            left_on="presenca",
            right_on="ID_presenca",
            drop_columns=["ID_presenca"],
        )
        df["presenca"] = df["Descricao"]
        df = df.drop(columns=["Descricao"])
    

    if "alfabetizado" in df.columns:
        df = enrich_dataframe(
            df=df,
            mapping_file="alfabetizado.csv",
            left_on="alfabetizado",
            right_on="id_alfabetizado",
            drop_columns=["id_alfabetizado"],
        )
        df["alfabetizado"] = df["descricao"]
        df = df.drop(columns=["descricao"])


    if "preenchimento_caderno" in df.columns:
        df = enrich_dataframe(
            df=df,
            mapping_file="preenchimento.csv",
            left_on="preenchimento_caderno",
            right_on="id_preenchimento",
            drop_columns=["id_preenchimento"],
        )
        df["preenchimento_caderno"] = df["descricao"]
        df = df.drop(columns=["descricao"])     


    if "sigla_uf" in df.columns:
        df = enrich_dataframe(
            df,
            mapping_file="uf.csv",
            left_on="sigla_uf",
            right_on="sigla",
            drop_columns=["sigla_uf"]
        )

        df = df.rename(
            columns={
            "sigla": "UF",
            "nome": "Nome_UF"
            }
        )


    if "id_municipio" in df.columns:
        df = enrich_dataframe(
            df,
            mapping_file="municipios.csv",
            left_on="id_municipio",
            right_on="municipio_id",
            drop_columns=["municipio_id"]
        )

    return df

