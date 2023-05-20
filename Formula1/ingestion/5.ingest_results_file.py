# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), False),

])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json('/mnt/lukaszdrozdformula1/raw/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns 

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_timestamp", current_timestamp()) 

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. statusId

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col('statusId'))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/lukaszdrozdformula1/processed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/processed/results

# COMMAND ----------

display(spark.read.parquet('/mnt/lukaszdrozdformula1/processed/results'))
