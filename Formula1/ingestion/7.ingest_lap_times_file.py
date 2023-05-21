# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest lapt_times.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the CSV file splited into mulifiles, using the spark dataframe reader 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False), \
    StructField("driverId", IntegerType(), True), \
    StructField("lap", IntegerType(), True), \
    StructField("position", IntegerType(), True), \
    StructField("time", StringType(), False), \
    StructField("milliseconds", IntegerType(), True) \
    
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv('/mnt/lukaszdrozdformula1/raw/lap_times')

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add Ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
                                               .withColumnRenamed('driverId', 'driver_id') \
                                               .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/lap_times

# COMMAND ----------


