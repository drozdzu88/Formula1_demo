# Databricks notebook source
# MAGIC %md
# MAGIC ##Mount Azure Data Lake using Service Principal
# MAGIC ###Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 1. Set Spark Config with App/ Client id, Direcotory/ Tenant id & Secert
# MAGIC 1. Call file system utility mount to mount the storage 
# MAGIC 1. Explore other file systems utilities related to mount (list all mounts, unmount)

# COMMAND ----------

dbutils.secrets.list(scope= 'formula1-scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='lukaszdrozdforumla1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@lukaszdrozdformula1.dfs.core.windows.net/",
  mount_point = "/mnt/lukaszdrozdformula1/demo",
  extra_configs = configs)

# COMMAND ----------

# dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("/mnt/lukaszdrozdformula1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/lukaszdrozdformula1/demo/circuits.csv"))

# COMMAND ----------

# listed all the mounts
display(dbutils.fs.mounts())


# COMMAND ----------

#unmount - remove your mount 
dbutils.fs.unmount('/mnt/lukaszdrozdformula1/demo')

# COMMAND ----------


