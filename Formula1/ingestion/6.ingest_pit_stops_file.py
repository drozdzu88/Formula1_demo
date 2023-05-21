# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON file using the spark dataframe reader with multiline option

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False), \
    StructField("driverId", IntegerType(), False), \
    StructField("stop", IntegerType(), False), \
    StructField("lap", IntegerType(), False), \
    StructField("time", StringType(), False), \
    StructField("duration", StringType(), True), \
    StructField("milliseconds", IntegerType(), True) \
    
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiline", True) \
.json('/mnt/lukaszdrozdformula1/raw/pit_stops.json')

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add Ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                                               .withColumnRenamed('driverId', 'driver_id') \
                                               .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Write output to parquet file

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/pit_stops
