from csv import reader
from collections import defaultdict
import os
import time
from pathlib import Path
from airflow.decorators import dag, task
from datetime import datetime

# Configurações globais
DATA_PATH = Path("/usr/local/airflow/include/measurements.txt")
OUTPUT_PATH = Path("/usr/local/airflow/include/measurements_summary.txt")

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 3},
    tags=["python", "etl"],
)
def python_airflow_etl():

    @task()
    def extract():
        """ Lê o arquivo CSV e retorna um dicionário com as temperaturas por estação. """
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"🚨 Arquivo não encontrado: {DATA_PATH}")

        temperatura_por_station = defaultdict(list)

        with open(DATA_PATH, 'r', encoding="utf-8") as file:
            _reader = reader(file, delimiter=';')
            for row in _reader:
                nome_da_station, temperatura = str(row[0]), float(row[1])
                temperatura_por_station[nome_da_station].append(temperatura)

        print(f"✅ Extração concluída. {len(temperatura_por_station)} estações carregadas.")
        return temperatura_por_station

    @task()
    def transform(temperatura_por_station: dict):
        """ Calcula estatísticas (min, média, max) e ordena os resultados. """
        results = {
            station: (
                min(temperatures),
                sum(temperatures) / len(temperatures),
                max(temperatures),
            )
            for station, temperatures in temperatura_por_station.items()
        }

        sorted_results = dict(sorted(results.items()))  # Ordena pelo nome da estação
        formatted_results = {
            station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
            for station, (min_temp, mean_temp, max_temp) in sorted_results.items()
        }

        print(f"✅ Transformação concluída. {len(formatted_results)} registros processados.")
        return formatted_results

    @task()
    def load(formatted_results: dict):
        """ Salva os resultados processados em um arquivo de texto. """
        with open(OUTPUT_PATH, 'w', encoding="utf-8") as file:
            for station, stats in formatted_results.items():
                file.write(f"{station}: {stats}\n")

        print(f"✅ Dados salvos com sucesso em {OUTPUT_PATH}")

    # Pipeline ETL
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

python_airflow_etl()
