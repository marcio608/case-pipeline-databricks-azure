# Databricks notebook source
# MAGIC %md
# MAGIC #### Verificando se os dados foram montados e os acessos a pasta inbound

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Lendo os dados da camada inbound

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/dbfs/mnt/dados/inbound'))

# COMMAND ----------

# MAGIC %scala
# MAGIC // criando um df 
# MAGIC val path = "dbfs:/FileStore/dbfs/mnt/dados/inbound/dados_brutos_imoveis.json"
# MAGIC val dados = spark.read.json(path)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Eliminar colunas indesejadas

# COMMAND ----------

# MAGIC %scala
# MAGIC // Eliminando as colunas imagens e usuarios
# MAGIC
# MAGIC val dados_anuncio = dados.drop("imagens", "usuario")
# MAGIC
# MAGIC display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Criar um campo de ID

# COMMAND ----------

# MAGIC %scala
# MAGIC // Usaremos o campo id na coluna anuncio para criar uma nova coluna chamada id
# MAGIC
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC
# MAGIC val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
# MAGIC
# MAGIC display(df_bronze)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC display(dbutils.fs.ls("/mnt/dados/"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Salvando na camada bronze

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)
# MAGIC

# COMMAND ----------


