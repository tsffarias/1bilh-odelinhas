import polars as pl
import os
from airflow.decorators import dag, task
from datetime import datetime

DATA_PATH = "/usr/local/airflow/include/measurements.txt"
OUTPUT_PATH = "/usr/local/airflow/include/measurements_summary.parquet"
EXTRACT_PATH = "/usr/local/airflow/include/extracted_measurements.parquet"
TRANSFORM_PATH = "/usr/local/airflow/include/transformed_measurements.parquet"
STREAMING_CHUNK_SIZE = 5_000

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
        """Lê dados com Polars e salva um parquet local. Retorna o caminho do parquet."""
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        # Ajusta chunk size de streaming
        pl.Config.set_streaming_chunk_size(STREAMING_CHUNK_SIZE)

        df = pl.scan_csv(
            DATA_PATH,
            separator=";",
            has_header=False,
            new_columns=["station", "measure"],
            schema={"station": pl.Utf8, "measure": pl.Float64}
        )

        # Coleta em DataFrame e salva (streaming=True se quiser forçar streaming)
        final_df = df.collect(streaming=True)
        final_df.write_parquet(EXTRACT_PATH)

        print(f"✅ Extração concluída. Salvando em {EXTRACT_PATH}")
        return EXTRACT_PATH  # Retorna uma string (OK para XCom)

    @task()
    def transform(parquet_path: str):
        """Carrega parquet, faz transform em Polars, salva result e retorna caminho."""
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"🚨 Arquivo do extract não encontrado: {parquet_path}")

        df = pl.read_parquet(parquet_path)
        transformed_df = (
            df.lazy()
            .group_by("station")
            .agg([
                pl.col("measure").max().alias("max"),
                pl.col("measure").min().alias("min"),
                pl.col("measure").mean().alias("mean"),
            ])
            .sort("station")
            .collect()
        )

        transformed_df.write_parquet(TRANSFORM_PATH)
        print(f"✅ Transformação concluída. Salvando em {TRANSFORM_PATH}")
        return TRANSFORM_PATH

    @task()
    def load(parquet_path: str):
        """Carrega parquet final e salva em OUTPUT_PATH."""
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"🚨 Arquivo transform não encontrado: {parquet_path}")

        final_df = pl.read_parquet(parquet_path)
        final_df.write_parquet(OUTPUT_PATH)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    extracted_path = extract()
    transformed_path = transform(extracted_path)
    load(transformed_path)

polars_airflow_etl()
