# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope= 'formula1-scope', key='lukaszdrozdformula1-account-key')

# COMMAND ----------


