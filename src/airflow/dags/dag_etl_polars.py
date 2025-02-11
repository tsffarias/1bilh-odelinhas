import polars as pl
import os
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = "/usr/local/airflow/include/measurements.txt"
OUTPUT_PATH = "/usr/local/airflow/include/measurements_summary.parquet"
STREAMING_CHUNK_SIZE = 4_000_000  # Tamanho do chunk para processamento em streaming

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["polars", "etl"],
)
def polars_airflow_etl():

    @task()
    def extract():
        """ Lê os dados do CSV usando Polars (streaming) e retorna um LazyFrame. """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        # Define o tamanho do chunk para leitura em streaming
        pl.Config.set_streaming_chunk_size(STREAMING_CHUNK_SIZE)

        df = pl.scan_csv(
            DATA_PATH, 
            separator=";", 
            has_header=False, 
            new_columns=["station", "measure"], 
            schema={"station": pl.Utf8, "measure": pl.Float64}
        )

        print(f"✅ Extração concluída. Streaming chunk size: {STREAMING_CHUNK_SIZE}")
        return df

    @task()
    def transform(df: pl.LazyFrame):
        """ Agrega os dados agrupando por estação. """
        transformed_df = (
            df.group_by("station")
            .agg([
                pl.col("measure").max().alias("max"),
                pl.col("measure").min().alias("min"),
                pl.col("measure").mean().alias("mean")
            ])
            .sort("station")
        )

        print(f"✅ Transformação concluída.")
        return transformed_df

    @task()
    def load(transformed_df: pl.LazyFrame):
        """ Coleta os dados e salva em formato Parquet. """
        final_df = transformed_df.collect(streaming=True)  # Converte LazyFrame em DataFrame
        final_df.write_parquet(OUTPUT_PATH)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

polars_airflow_etl()
