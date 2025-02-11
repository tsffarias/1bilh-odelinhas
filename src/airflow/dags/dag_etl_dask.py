import dask
import dask.dataframe as dd
import os
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = "/usr/local/airflow/include/measurements.txt"
OUTPUT_PATH = "/usr/local/airflow/include/measurements_summary.parquet"

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
        """ Lê o arquivo CSV usando Dask e retorna um DataFrame distribuído. """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        dask.config.set({'dataframe.query-planning': True})  # Configura otimização de execução

        df = dd.read_csv(DATA_PATH, sep=";", header=None, names=["station", "measure"])
        print(f"✅ Extração concluída. {df.npartitions} partições carregadas.")
        return df

    @task()
    def transform(df: dd.DataFrame):
        """ Agrega os dados agrupando por estação e calcula estatísticas. """
        grouped_df = df.groupby("station")['measure'].agg(['max', 'min', 'mean']).reset_index()
        print(f"✅ Transformação concluída. Operações registradas para execução.")
        return grouped_df

    @task()
    def load(transformed_df: dd.DataFrame):
        """ Computa e salva o resultado final em Parquet. """
        result_df = transformed_df.compute().sort_values("station")  # Executa as operações
        result_df.to_parquet(OUTPUT_PATH, index=False)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

dask_airflow_etl()
