# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Spotify: Bronze -> Silver -> Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Camada Bronze — dado bruto

# COMMAND ----------

# Criando um schema com pyspark

spark.sql("CREATE SCHEMA IF NOT EXISTS files.spotify_files")

# COMMAND ----------

# Lendo novamente o csv do notebook anterior

from pyspark.sql import functions as F

df = spark.read.csv(
    "/Volumes/files/spotify_files/volumes/spotify_artist_streaming_2020_2025.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

# Sintaxe do .write para criar tabela Delta
# df.write.format("delta").mode("overwrite").saveAsTable("nome_do_catalogo.nome_do_schema.nome_da_tabela")
# .write = serve para gravar dados
# .format = especifica o formato de dados
# .mode = define o modo de gravacao
# .saveAsTable = cria uma tabela no catalogo
# .overwrite = substitui os dados existentes da tabela

df.write.format("delta").mode("overwrite").saveAsTable("files.spotify_files.spotify_bronze")

# COMMAND ----------

# verificando se a tabela foi criada

spark.sql("SELECT * FROM files.spotify_files.spotify_bronze").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camada Silver — transformações validadas

# COMMAND ----------

# lendo da camada Bronze

df_bronze = spark.sql("SELECT * FROM files.spotify_files.spotify_bronze")

# aplicando a transformação

df_silver = df_bronze.withColumn("Periodo",
    F.when(F.col("release_year").isin(2020, 2021), "Pandemia (2020-2021)").otherwise("Pós Pandemia (2022+)")
)

# removendo a coluna duplicada com drop

df_silver = df_silver.drop("explicit")

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").saveAsTable("files.spotify_files.spotify_silver")

# COMMAND ----------


df_silver_show = spark.sql("SELECT * FROM files.spotify_files.spotify_silver")
df_silver_show.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camada Gold — agregações para consumo

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM files.spotify_files.spotify_silver")

df_gold_genero = df_silver.groupBy("genre") \
    .agg(
        F.count("*").alias("qtd_musicas"),
        F.expr("percentile_approx(stream_count, 0.5)").alias("mediana_streams")
    ) \
        .orderBy(F.desc("mediana_streams"))

df_gold_genero.show()

# COMMAND ----------

df_gold_genero.write.format("delta").mode("overwrite").saveAsTable("files.spotify_files.spotify_gold")

# COMMAND ----------

df_gold_genero = spark.read.table("files.spotify_files.spotify_gold")
df_gold_genero.show()

# COMMAND ----------

df_gold_genero = spark.sql("SELECT * FROM files.spotify_files.spotify_gold")
df_gold_genero.show()

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM files.spotify_files.spotify_silver")

df_gold_periodo = df_silver.groupBy("Periodo") \
    .agg(
        F.count("*").alias("Qtd_musica"),
        F.expr("percentile_approx(stream_count, 0.5)").alias("mediana_streams")
    ) \
        .orderBy(F.desc("mediana_streams"))

df_gold_periodo.show()

# COMMAND ----------

df_gold_periodo.write.format("delta").mode("overwrite").saveAsTable("files.spotify_files.spotify_gold_periodo")
