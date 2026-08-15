# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Documentação e Contexto
# MAGIC %md
# MAGIC ## Tratamento de Funcionários
# MAGIC Este passo da pipeline remove colunas sensíveis e seleciona apenas:
# MAGIC * O **ID** do funcionário
# MAGIC * O **Nome** do funcionário

# COMMAND ----------

# DBTITLE 1,Importação de Bibliotecas
import pyspark.sql.functions as F

# Importando apenas o módulo de funções do PySpark para otimizar memória

# COMMAND ----------

# DBTITLE 1,Leitura da Tabela Silver
# Lendo a tabela direto do Unity Catalog / Metastore
df = spark.read.table("dados_rh.`2_silver`.silver_funcionarios")

# COMMAND ----------


# DBTITLE 1,Tratamento de Dados
def tratar_dados_funcionarios(dataframe):
    """
    Filtra o dataframe mantendo apenas as colunas 'id' e 'funcionario'.
    """
    return dataframe.select(F.col("id"), F.col("funcionario"))


df_tratado = tratar_dados_funcionarios(df)

# COMMAND ----------

# DBTITLE 1,Exibição do Dataframe
display(df_tratado)
