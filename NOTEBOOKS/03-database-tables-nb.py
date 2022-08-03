# Databricks notebook source
# MAGIC %md
# MAGIC # DATA OBJECT CREATION

# COMMAND ----------

dbutils.widgets.text("environmentWidget","","Select Environment: ")
environmentVariable = dbutils.widgets.get("environmentWidget")
print(environmentVariable)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Register the Tables 

# COMMAND ----------

spark.sql(f"USE {environmentVariable}_acme_inc_db")

# COMMAND ----------

spark.sql(f"""
        CREATE OR REPLACE TABLE 02_silver_orders
        AS 
        SELECT * 
        FROM DELTA.`dbfs:/acme-company/02-silver-layer/`
""")

# COMMAND ----------

spark.sql(f"""
        CREATE OR REPLACE TABLE 03_gold_orders
        AS 
        SELECT * 
        FROM DELTA.`dbfs:/acme-company/03-gold-layer/`
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- END OF STEP 3 --
