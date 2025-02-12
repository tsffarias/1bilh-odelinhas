import dask
import dask.dataframe as dd
import os
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = "/usr/local/airflow/include/measurements.txt"           # CSV original
EXTRACT_PATH = "/usr/local/airflow/include/extracted_measurements"  # folder para salvarmos parquet do extract
TRANSFORM_PATH = "/usr/local/airflow/include/transformed_measurements"  # folder parquet transformado
OUTPUT_PATH = "/usr/local/airflow/include/final_measurements_summary.parquet"  # resultado final

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["dask", "etl"],
)
def dask_airflow_etl():
    @task()
    def extract():
        """Lê o CSV em Dask, salva em formato Parquet e retorna o caminho."""
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        # Opcional: setar configs antes de importar dask.dataframe
        dask.config.set({'dataframe.query-planning': True})

        # Lê csv como Dask DataFrame
        df = dd.read_csv(DATA_PATH, sep=';', header=None, names=['station', 'measure'])
        print(f"✅ Extração concluída. {df.npartitions} partições carregadas.")

        # Salva no formato Parquet (pode gerar vários arquivos na pasta EXTRACT_PATH)
        df.to_parquet(EXTRACT_PATH)
        print(f'✅ Dados extraídos salvos em: {EXTRACT_PATH}')

        # Retorna somente o path (string) para o XCom
        return EXTRACT_PATH

    @task()
    def transform(parquet_path: str):
        """Carrega Parquet, faz transform (groupby) e salva transformado em outro Parquet."""
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {parquet_path}")

        df = dd.read_parquet(parquet_path)
        # Registra a operação, mas ainda não executa
        grouped_df = df.groupby("station")["measure"].agg(["max", "min", "mean"])

        # Salva o resultado transformado em outro parquet
        grouped_df.to_parquet(TRANSFORM_PATH)
        print(f"✅ Transformação concluída. Arquivos salvos em {TRANSFORM_PATH}")

        return TRANSFORM_PATH

    @task()
    def load(parquet_path: str):
        """Carrega parquet final, executa o compute() e salva como parquet único final."""
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f'🚨 Arquivo transformado não encontrado: {parquet_path}')

        df = dd.read_parquet(parquet_path)
        # Aqui executamos o compute e reordenamos por 'station'
        final_df = df.compute().sort_values("station")
        final_df.to_parquet(OUTPUT_PATH, index=False)
        print(f"✅ Resultado final salvo em {OUTPUT_PATH}")

    # Pipeline ETL
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)

dask_airflow_etl()
