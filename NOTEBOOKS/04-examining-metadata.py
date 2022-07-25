# Databricks notebook source
df = (
  spark.read
       .format("json")
       .option("inferSchema", "true")
       .load("dbfs:/acme-company/01-bronze-layer/_delta_log/00000000000000000001.json")
       .select("add")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.select(df.add.modificationTime, df.add.size, df.add.stats))
