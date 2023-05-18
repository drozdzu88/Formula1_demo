# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID')

# COMMAND ----------

for file in dbutils.fs.ls('/databricks-datasets/COVID'):
    if file.name.endswith('/'):
        print(file.name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------


