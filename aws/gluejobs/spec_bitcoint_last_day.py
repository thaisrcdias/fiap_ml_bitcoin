import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, to_date
from datetime import datetime, timedelta

# Inicializar Spark
sc = SparkContext()
spark = SparkSession(sc)

# Caminho do bucket S3
input_bucket = "s3://raw-209112358514/bitcoin/"
output_bucket = "s3://refined-209112358514/bitcoin_max_close/"

# Calcular a data do dia anterior
yesterday = datetime.now() - timedelta(days=1)
year = yesterday.year
month = yesterday.month
day = yesterday.day

# Construir o caminho do dia anterior no S3
input_path = f"{input_bucket}{year}/{month}/{day}/"

# Ler os dados Parquet do dia anterior
try:
    df = spark.read.parquet(input_path)
    print(f"Dados carregados do caminho: {input_path}")
except Exception as e:
    print(f"Erro ao carregar os dados do caminho {input_path}: {e}")
    sys.exit(1)

# Converter a coluna 'date' para o formato de data (se necessário)
df = df.withColumn("date", to_date(col("date")))

# Calcular o valor máximo da coluna 'close' para o dia anterior
df_max_close = df.groupBy("date").agg(max("close").alias("max_close"))

# Salvar o resultado no S3
output_path = f"{output_bucket}{year}/{month}/{day}/"
df_max_close.write.mode("overwrite").parquet(output_path)

print(f"Job concluído! Resultados salvos em: {output_path}")