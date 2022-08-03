# Databricks notebook source
# MAGIC %md
# MAGIC # TRANSFORMATION

# COMMAND ----------

dbutils.widgets.text("environmentWidget","","Select Environment: ")
environmentVariable = dbutils.widgets.get("environmentWidget")
print(environmentVariable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest the ```BRONZE``` Table 

# COMMAND ----------

df = (
  spark.table(f"{environmentVariable}_acme_inc_db.01_bronze_orders")
)
display(df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform into ```SILVER``` Table 

# COMMAND ----------

df.createOrReplaceTempView("sql_df")

# COMMAND ----------

silverDF = spark.sql("""
  SELECT 
      orderID, 
      TO_DATE(orderDate, "yyyy-MM-dd") AS order_date, 
      category, 
      subCategory AS sub_category, 
      productName AS product_name, 
      CAST(sales AS NUMERIC) as sales_revenue, 
      CAST(quantity AS INTEGER) AS quantity, 
      CAST(discount AS NUMERIC) AS discount 
  FROM 
      sql_df
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Transform into ```GOLD``` Table 

# COMMAND ----------

goldDF = spark.sql("""
  SELECT
      category, 
      subCategory AS sub_category, 
      productName AS product_name, 
      SUM(sales) AS total_sales
  FROM 
      sql_df
  GROUP BY 1,2,3
  ORDER BY 4 DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the Tables to Delta 

# COMMAND ----------

silverTablePath = "dbfs:/acme-company/02-silver-layer/"
(
  silverDF.write
          .format("delta")
          .mode("overwrite")
          .save(silverTablePath)
)

# COMMAND ----------

goldTablePath = "dbfs:/acme-company/03-gold-layer/"
(
  goldDF.write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "false")
          .save(goldTablePath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- END OF STEP 2 -- 
