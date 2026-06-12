# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Teste de Sincronização do Time - LH Nautical
# MAGIC Se você está conseguindo ler esta tela dentro do Databricks, parabéns! O pipeline do Bitbucket está funcionando.

# COMMAND ----------
print("Hello World! O código veio da pasta tests/ do Bitbucket direto para o Databricks! 🎉")

# COMMAND ----------
print("Testando o motor do Apache Spark...")
df_teste = spark.range(1, 6)
display(df_teste)
