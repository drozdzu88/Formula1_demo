# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)

])

# COMMAND ----------

driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)

])

# COMMAND ----------

drivers_df = spark.read \
.schema(driver_schema) \
.json('/mnt/lukaszdrozdformula1/raw/drivers.json')

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns 
# MAGIC 1. driverid renamed to driver_id
# MAGIC 1. driverRef renamed to driver_ref
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_timestamp", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/drivers

# COMMAND ----------


