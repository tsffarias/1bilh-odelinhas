import duckdb
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
    tags=["duckdb", "etl"],
)
def duckdb_airflow_etl():

    @task()
    def extract():
        """ Lê os dados do arquivo CSV usando DuckDB. """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        conn = duckdb.connect(database=":memory:")
        conn.execute("PRAGMA threads=16;")  # Usa 16 núcleos
        conn.execute("PRAGMA memory_limit='12GB';")  # Define limite de memória

        # Lê os dados
        query = f"""
            SELECT * FROM read_csv('{DATA_PATH}', AUTO_DETECT=FALSE, sep=';', 
                                   columns={{'station': 'VARCHAR', 'temperature': 'DECIMAL(3,1)'}})
        """
        df = conn.sql(query).fetchdf()
        print(f"✅ Extração concluída. {len(df)} registros carregados.")
        return df

    @task()
    def transform(df):
        """ Aplica transformação e agregação nos dados extraídos. """
        conn = duckdb.connect(database=":memory:")
        conn.register("df", df)  # Registra DataFrame no DuckDB para manipulação

        transformed_query = """
            SELECT station,
                MIN(temperature) AS min_temperature,
                CAST(AVG(temperature) AS DECIMAL(3,1)) AS mean_temperature,
                MAX(temperature) AS max_temperature
            FROM df
            GROUP BY station
            ORDER BY station
        """
        result = conn.sql(transformed_query).fetchdf()
        print(f"✅ Transformação concluída. {len(result)} registros agregados.")
        return result

    @task()
    def load(transformed_df):
        """ Salva o DataFrame transformado em um arquivo Parquet. """
        duckdb.sql("INSTALL parquet; LOAD parquet;")  # Garante suporte ao Parquet
        duckdb.from_df(transformed_df).to_parquet(OUTPUT_PATH)
        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

duckdb_airflow_etl()