"""
Exemplo de DAG usando paralelismo via várias tasks para processar chunks no Airflow.
"""

import os
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task

# =========================
# Configurações Globais
# =========================
DATA_PATH = "/usr/local/airflow/include/measurements.txt"           # Path do arquivo original
OUTPUT_PATH_FINAL = "/usr/local/airflow/include/final_summary.parquet"  # Resultado final
TMP_FOLDER = "/usr/local/airflow/include/tmp_chunks"                # Onde salvaremos os parquets parciais
TOTAL_LINHAS = 1_000_000_000  # Numero total de linhas esperado
CHUNKSIZE = 5_000_000       # Tamanho de cada chunk

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["pandas", "etl", "parallel"],
)
def pandas_parallel_chunks():
    """
    DAG de exemplo que demonstra como quebrar o processamento em várias tasks.
    Cada task processa um chunk do dataset, e no final unificamos os resultados.
    """

    @task()
    def create_chunk_list():
        # Quantos chunks serão processados?
        total_chunks = TOTAL_LINHAS // CHUNKSIZE + (1 if TOTAL_LINHAS % CHUNKSIZE != 0 else 0)
        return list(range(total_chunks))

    @task()
    def process_chunk(chunk_index: int):
        """
        Lê apenas as linhas correspondentes ao 'chunk_index', realiza a agregação e
        salva o resultado em um arquivo parquet parcial.
        """
        # Se a pasta TMP_FOLDER não existir, cria
        if not os.path.exists(TMP_FOLDER):
            os.makedirs(TMP_FOLDER, exist_ok=True)

        start_row = chunk_index * CHUNKSIZE
        nrows = CHUNKSIZE

        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"Arquivo não encontrado: {DATA_PATH}")

        # Lê o pedaço do CSV usando skiprows e nrows
        df = pd.read_csv(
            DATA_PATH,
            sep=';',
            header=None,
            names=['station', 'measure'],
            skiprows=range(1, start_row + 1),  # Pula linhas até o ponto inicial
            nrows=nrows
        )

        # Agrega
        df_agg = df.groupby('station')["measure"].agg(['min', 'max', 'mean']).reset_index()

        # Salva parcial
        partial_path = os.path.join(TMP_FOLDER, f"measurements_summary_{chunk_index}.parquet")
        df_agg.to_parquet(partial_path, index=False)

        return partial_path

    @task()
    def merge_parquets(partial_paths: list):
        """
        Lê todos os parquets parciais gerados e faz o merge (nova agregação),
        salvando o resultado final em OUTPUT_PATH_FINAL.
        """
        if not partial_paths:
            raise ValueError("Nenhum arquivo parcial foi gerado para merge.")

        all_dfs = []
        for path in partial_paths:
            df_temp = pd.read_parquet(path)
            all_dfs.append(df_temp)

        final_df = pd.concat(all_dfs, ignore_index=True)
        # Faz a agregação final
        final_aggregated_df = (
            final_df.groupby('station')
                    .agg({'min': 'min', 'max': 'max', 'mean': 'mean'})
                    .reset_index()
                    .sort_values('station')
        )
        # Salva
        final_aggregated_df.to_parquet(OUTPUT_PATH_FINAL, index=False)
        print(f"✅ Resultado final salvo em {OUTPUT_PATH_FINAL}")

    # 1. Gerar a lista de chunks
    chunk_list = create_chunk_list()

    # 2. Rodar process_chunk em paralelo para cada chunk
    partial_files = process_chunk.expand(chunk_index=chunk_list)

    # 3. Fazer o merge final após todas as tasks anteriores concluírem
    merge_parquets(partial_files)

dag_instance = pandas_parallel_chunks()
