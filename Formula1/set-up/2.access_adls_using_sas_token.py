# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.lukaszdrozdformula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.lukaszdrozdformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# SAS Token expires after 8h
spark.conf.set("fs.azure.sas.fixed.token.lukaszdrozdformula1.dfs.core.windows.net", "sp=rl&st=2023-05-19T08:22:04Z&se=2023-05-19T16:22:04Z&spr=https&sv=2022-11-02&sr=c&sig=OXVQRqzJ4uz8PFLFDRELtBcf79oeXzubdctej6w1V%2FI%3D") 

# COMMAND ----------

# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


