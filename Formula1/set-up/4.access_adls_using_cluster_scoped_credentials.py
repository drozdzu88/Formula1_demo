# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using cluster scoped credentials
# MAGIC ###Steps to follow
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net/circuits.csv"))
