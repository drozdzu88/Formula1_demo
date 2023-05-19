# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.lukaszdrozdformula1.dfs.core.windows.net",
    "<acces key from storage account>"
)

# COMMAND ----------

# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


