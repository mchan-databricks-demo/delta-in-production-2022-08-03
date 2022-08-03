# Databricks notebook source
# MAGIC %md
# MAGIC # INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Setup the environment

# COMMAND ----------

dbutils.widgets.text("environmentWidget","","Select Environment: ")
environmentVariable = dbutils.widgets.get("environmentWidget")
print(environmentVariable)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {environmentVariable}_acme_inc_db")
spark.sql(f"USE {environmentVariable}_acme_inc_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Examine the raw data 

# COMMAND ----------

# we have 4 years' worth of sales data to ingest 
display(dbutils.fs.ls("dbfs:/FileStore/workshop_sample_data/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest the data using Databricks Autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC #### Goals:
# MAGIC ##### ✓ Ingest data with a real-time Medallion Architecture for managing data 
# MAGIC ##### ✓ DevOps / Environment management (DEV, PROD)
# MAGIC ##### ✓ Scheduling and orchestrating jobs through tasks 
# MAGIC ##### ✓ Debugging and performance tuning with Delta + Photon

# COMMAND ----------

# Path to our raw files
dbfsPath = "dbfs:/FileStore/workshop_sample_data/"

# Autoloader 
# Incrementally ingest new data files as they arrive without additional setup 
# Provides a streaming source called cloudFiles
# Schema Inference and Schema Evolution 
df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", "dbfs:/tmp/checkpointPath")
         .option("cloudFiles.inferColumnTypes", "true")   
         .load(dbfsPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Add helper metadata columns 

# COMMAND ----------

from pyspark.sql.functions import * 

df = (
    df.withColumn("file_path", input_file_name())
      .withColumn("ingest_time", current_timestamp())
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the ingested data to the ```BRONZE``` layer 

# COMMAND ----------

bronzeCheckpointPath = "dbfs:/acme-company/01-bronze-checkpoint/"
bronzeTablePath = "dbfs:/acme-company/01-bronze-layer/"

(
    df.writeStream
      .format("delta")
      .option("checkpointLocation", bronzeCheckpointPath)
      .trigger(availableNow = True)
      .option("path", bronzeTablePath)
      .outputMode("append")
      .table(f"{environmentVariable}_acme_inc_db.01_bronze_orders")   
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- END OF STEP 1 --
