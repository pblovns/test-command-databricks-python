# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.table("dados_rh.`2_silver`.silver_funcionarios")

# COMMAND ----------

df_tratado = (df.select(F.col('id'), F.col('funcionario')))

# COMMAND ----------

display(df_tratado)