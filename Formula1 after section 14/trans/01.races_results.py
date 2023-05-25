# Databricks notebook source
# MAGIC %md
# MAGIC ### Race Results presentation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Createing dataframes for join operation
# MAGIC 1. 2020 Abu Dhabi Grand Prix Results
# MAGIC 1. Race Yas Marina, 13 December 2020

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
                        .withColumnRenamed("time", "race_time")


# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                        .withColumnRenamed("name", "race_name") \
                        .withColumnRenamed("race_timestamp", "race_date") 
                    

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("location", "circuit_location")


# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
                        .withColumnRenamed("name", "driver_name") \
                        .withColumnRenamed("number", "driver_number") \
                        .withColumnRenamed("nationality", "driver_nationality") 


# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
                        .withColumnRenamed("name", "team")
                            

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Joins

# COMMAND ----------

race_circuits_join_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")

# COMMAND ----------

race_results_df = results_df.join(race_circuits_join_df, results_df.race_id == race_circuits_join_df.race_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")
                            

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                  "team", "grid", "fastest_lap", "race_time", "points", "position") \
                        .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to the parquet file

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------


