# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Importação de Bibliotecas
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Leitura da Tabela Silver
df = spark.read.table("dados_rh.`2_silver`.silver_funcionarios")

# COMMAND ----------

# DBTITLE 1,Tratamento de Dados
df_tratado = (df.select(F.col('id'), F.col('funcionario')))

# COMMAND ----------

# DBTITLE 1,Exibição do Dataframe
display(df_tratado)
