# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest qualifing.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the set of JSON files using the spark dataframe reader 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False), \
    StructField("raceId", IntegerType(), False), \
    StructField("driverId", IntegerType(), False), \
    StructField("constructorId", IntegerType(), False), \
    StructField("number", IntegerType(), False), \
    StructField("position", IntegerType(), True), \
    StructField("q1", StringType(), True), \
    StructField("q2", StringType(), True), \
    StructField("q3", StringType(), True)
    
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json('/mnt/lukaszdrozdformula1/raw/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add Ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('raceId', 'race_id') \
                                               .withColumnRenamed('driverId', 'driver_id') \
                                               .withColumnRenamed('qualifyId', 'qualify_id') \
                                               .withColumnRenamed('constructorId', 'constructor_id') \
                                               .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/qualifying

# COMMAND ----------


