# Databricks notebook source
# MAGIC %md  
# MAGIC ###Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------


