# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake using Service Principal
# MAGIC ###Steps to follow
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client id, Directory/Tenant id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

dbutils.secrets.list(scope= 'formula1-scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.lukaszdrozdformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.lukaszdrozdformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.lukaszdrozdformula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.lukaszdrozdformula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.lukaszdrozdformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@lukaszdrozdformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


