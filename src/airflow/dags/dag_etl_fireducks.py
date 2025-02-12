import fireducks.pandas as pd
import os
from tqdm import tqdm
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = "/usr/local/airflow/include/measurements.txt"
OUTPUT_PATH = "/usr/local/airflow/include/measurements_summary.parquet"
TOTAL_LINHAS = 1_000_000_000
CHUNKSIZE = 100_000_000

# Caminhos intermediários
EXTRACT_PATH = "/usr/local/airflow/include/fireducks_extract.parquet"
TRANSFORM_PATH = "/usr/local/airflow/include/fireducks_transform.parquet"

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["fireducks", "etl"],
)
def fireducks_airflow_etl():

    @task()
    def extract():
        """
        Lê o arquivo em chunks, concatena e salva como Parquet.
        Retorna apenas o caminho (string), não o DataFrame.
        """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        total_chunks = TOTAL_LINHAS // CHUNKSIZE + (1 if TOTAL_LINHAS % CHUNKSIZE else 0)
        chunks = []

        reader = pd.read_csv(DATA_PATH, sep=';', header=None, names=['station', 'measure'], chunksize=CHUNKSIZE)

        for chunk in tqdm(reader, total=total_chunks, desc="📥 Extraindo dados"):
            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)
        df.to_parquet(EXTRACT_PATH, index=False)

        print(f"✅ Extração concluída. {len(chunks)} chunks carregados de um total estimado de {total_chunks}.")
        print(f"✅ Dados extraídos salvos em {EXTRACT_PATH}")
        return EXTRACT_PATH  # Só o path

    @task()
    def transform(parquet_path: str):
        """
        Lê o Parquet extraído, processa e salva o DataFrame transformado em outro Parquet.
        Retorna apenas o path do Parquet transformado.
        """
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {parquet_path}")

        df = pd.read_parquet(parquet_path)
        
        # Processa em chunks novamente se quiser, mas aqui já está todo em DataFrame
        def process_chunk(chunk):
            return chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()

        # Para este volume, podemos processar direto ou em "lotes" manuais
        results = process_chunk(df)
        # Nova agregação final
        final_df = results.groupby('station').agg({
            'min': 'min',
            'max': 'max',
            'mean': 'mean'
        }).reset_index().sort_values('station')

        final_df.to_parquet(TRANSFORM_PATH, index=False)
        print(f"✅ Transformação concluída. {len(final_df)} registros agregados.")
        print(f"✅ Parquet transformado salvo em {TRANSFORM_PATH}")

        return TRANSFORM_PATH

    @task()
    def load(parquet_path: str):
        """ Lê o parquet transformado e salva em OUTPUT_PATH. """
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {parquet_path}")

        transformed_df = pd.read_parquet(parquet_path)
        transformed_df.to_parquet(OUTPUT_PATH, index=False)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    extracted_data_path = extract()
    transformed_data_path = transform(extracted_data_path)
    load(transformed_data_path)

fireducks_airflow_etl()
