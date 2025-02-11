import pandas as pd
import os
from multiprocessing import Pool, cpu_count
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = "/usr/local/airflow/include/measurements.txt"
OUTPUT_PATH = "/usr/local/airflow/include/measurements_summary.parquet"
TOTAL_LINHAS = 1_000_000_000  # Número total de linhas esperado
CHUNKSIZE = 100_000_000  # Tamanho do chunk
CONCURRENCY = cpu_count()  # Número de núcleos disponíveis

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["pandas", "etl"],
)
def pandas_airflow_etl():

    @task()
    def extract():
        """ Lê os dados do CSV em chunks e retorna uma lista de DataFrames. """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        total_chunks = TOTAL_LINHAS // CHUNKSIZE + (1 if TOTAL_LINHAS % CHUNKSIZE else 0)
        chunks = []
        
        with pd.read_csv(DATA_PATH, sep=';', header=None, names=['station', 'measure'], chunksize=CHUNKSIZE) as reader:
            for chunk in reader:
                chunks.append(chunk)

        print(f"✅ Extração concluída. {len(chunks)} chunks carregados de um total estimado de {total_chunks}.")
        return chunks

    @task()
    def transform(chunks):
        """ Processa os dados em paralelo e agrega os valores por estação. """
        def process_chunk(chunk):
            return chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()

        with Pool(CONCURRENCY) as pool:
            results = pool.map(process_chunk, chunks)

        final_df = pd.concat(results, ignore_index=True)
        final_aggregated_df = final_df.groupby('station').agg({
            'min': 'min',
            'max': 'max',
            'mean': 'mean'
        }).reset_index().sort_values('station')

        print(f"✅ Transformação concluída. {len(final_aggregated_df)} registros agregados.")
        return final_aggregated_df

    @task()
    def load(transformed_df):
        """ Salva o DataFrame transformado em um arquivo Parquet. """
        transformed_df.to_parquet(OUTPUT_PATH, index=False)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

pandas_airflow_etl()
