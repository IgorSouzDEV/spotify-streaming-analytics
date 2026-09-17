# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregando os dados brutos

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/files/spotify_files/volumes/spotify_artist_streaming_2020_2025.csv",
    header=True,
    inferSchema=True
)
df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("track_id").distinct().count()

# COMMAND ----------

df.groupBy("genre").count().orderBy("count", ascending=False).show()

# COMMAND ----------

df.describe("stream_count", "popularity", "danceability", "energy").show()

# COMMAND ----------

df.approxQuantile("stream_count", [0.25, 0.5, 0.75, 0.9, 0.99], 0.01)

# COMMAND ----------

from pyspark.sql import functions as F

df.groupBy("genre") \
    .agg(F.expr("percentile_approx(stream_count, 0.5)").alias("mediana_streams")) \
        .orderBy(F.desc("mediana_streams")) \
            .show()

# COMMAND ----------

df.groupBy("genre") \
    .agg(
        F.count("*").alias("qtd_musicas"),
        F.expr("percentile_approx(stream_count, 0.5)").alias("mediana_streams")
    ) \
        .orderBy(F.desc("mediana_streams")) \
        .show()

# COMMAND ----------

# .agg = permite calcular as métricas
# ("*") = todos os registros
# .filter = permite filtrar
# .counter = contar a quantidade de registros
# .alias = renomear a coluna
# .orderBy = ordenar
# .desc = ordenar decrescente
# .expr = expressão SQL
# percentile_approx = calcular a mediana

from pyspark.sql import functions as F

df.filter("release_year >=2023").groupBy("genre") \
    .agg(
        F.count("*").alias("total"),
        F.expr("percentile_approx(stream_count, 0.5)").alias("mediana_streams")
    ) \
        .orderBy(F.desc("mediana_streams")).show()

# COMMAND ----------

# .withColumn = cria uma coluna com uma expressão SQL
# when = cria uma condição SQL
# otherwise = cria uma condição SQL para o caso contrário
# isin = cria uma condição SQL para verificar se um valor está em uma lista
# sintaxe do when = .when(condição, valor se verdadeiro).otherwise(valor se falso)
# sintaxe do isin = .isin(valor1, valor2, ..., valorN)

from pyspark.sql import functions as F

df_com_periodo = df.withColumn(
    "Periodo",
    F.when(F.col("release_year").isin(2020, 2021), "Pandemia (2020-2021)").otherwise("Pós pandemia (2022+)")
)
df_com_periodo.select("track_name", "release_year", "Periodo").show(10)

# COMMAND ----------

df_com_periodo.groupBy("Periodo").count().show()