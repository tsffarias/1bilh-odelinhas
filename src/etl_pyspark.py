from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, min, max, avg

# Lista com 300 cidades
cities = [
    # Adicione a lista de 300 cidades aqui
]

# Define o esquema da tabela (cidade e valor aleatório)
schema = StructType([
    StructField("city", StringType(), False),
    StructField("value", FloatType(), True),
])

# Gera os dados para 1 bilhão de linhas
num_rows = 1000000000

# Define a função para gerar valores aleatórios entre -99 e 99
def generate_random_value():
    return random.uniform(-99.0, 99.0)

# Cria o RDD com os dados
data = spark.sparkContext.parallelize(range(num_rows)) \
    .map(lambda x: (cities[x % len(cities)], generate_random_value()))

# Cria o DataFrame
df = spark.createDataFrame(data, schema)

# Mostra um amostra dos dados
df.show()

# Para escrever o DataFrame gerado em um arquivo, por exemplo, em formato parquet:
# df.write.parquet("caminho/para/salvar/tabela.parquet")


# Define o esquema da tabela para os resultados da agregação
agg_schema = StructType([
    StructField("city", StringType(), False),
    StructField("min_value", FloatType(), True),
    StructField("max_value", FloatType(), True),
    StructField("avg_value", FloatType(), True),
])

# Calcula os valores mínimos, máximos e médios agrupados por cidade
agg_df = df.groupBy("city") \
    .agg(min(col("value")).alias("min_value"),
         max(col("value")).alias("max_value"),
         avg(col("value")).alias("avg_value"))

# Mostra os resultados da agregação
agg_df.show()

# Para escrever o DataFrame agregado em um arquivo, por exemplo, em formato parquet:
# agg_df.write.parquet("caminho/para/salvar/tabela_agregada.parquet")
