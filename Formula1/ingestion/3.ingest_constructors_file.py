# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json('/mnt/lukaszdrozdformula1/raw/constructors.json')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_dropped_df = constructor_df.drop('url')

# COMMAND ----------

### other ways to drop columns
# constructors_dropped_df = constructor_df.drop(constructor_df['url']) - useful when we joined two tables/dataframes and each has 'url' column
# from spark.sql.funtions import col
# constructors_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                               .withColumnRenamed('constructorRef', 'constructor_ref') \
                                               .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/constructors

# COMMAND ----------


