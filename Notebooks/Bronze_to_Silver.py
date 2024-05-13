# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Lendo os dados na camada bronze
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
# MAGIC
# MAGIC val df = spark.read.format("delta").load(path)
# MAGIC
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Transformando os campos do JSON em colunas
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC
# MAGIC display(df.select("anuncio.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*", "anuncio.endereco.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")
# MAGIC display(dados_detalhados)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Excluindo a coluna caracteriscas e endereco

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
# MAGIC display(df_silver)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Salvando na camada Silver

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
# MAGIC df_silver.write.format("delta").mode("overwrite").save(path)
# MAGIC

# COMMAND ----------


