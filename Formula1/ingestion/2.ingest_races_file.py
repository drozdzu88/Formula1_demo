# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import col, to_timestamp, lit, concat, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1- Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/lukaszdrozdformula1/raw

# COMMAND ----------

races_df = spark.read.option("header", True).csv('dbfs:/mnt/lukaszdrozdformula1/raw/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False), \
                          StructField("year", IntegerType(), True), \
                          StructField("round", IntegerType(), True), \
                          StructField("circuitId", IntegerType(), False), \
                          StructField("name", StringType(), True), \
                          StructField("date", DateType(), True), \
                          StructField("time", StringType(), True), \
                          StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv('dbfs:/mnt/lukaszdrozdformula1/raw/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add two new columns

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
                                    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Select only the required columns 

# COMMAND ----------

final_races_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"))

# COMMAND ----------

display(final_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write as a parquet file to Data Lake Storage

# COMMAND ----------

final_races_df.write.mode("overwrite").parquet("/mnt/lukaszdrozdformula1/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/lukaszdrozdformula1/processed/races"))

# COMMAND ----------


