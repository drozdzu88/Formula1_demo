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
                        .withColumnRenamed("fastest_lap_time", "fastest lap") \
                        .withColumnRenamed("time", "race_time")


# COMMAND ----------

display(results_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                        .withColumnRenamed("name", "race_name") \
                        .withColumnRenamed("race_timestamp", "race_date") 
                    

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("location", "circuit_location")


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
                        .withColumnRenamed("name", "driver_name") \
                        .withColumnRenamed("number", "driver_number") \
                        .withColumnRenamed("nationality", "driver_nationality") 


# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
                        .withColumnRenamed("name", "team")
                            

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

races_filtered_df = races_df.filter((races_df.race_year == 2020) & (races_df.race_name == "Abu Dhabi Grand Prix"))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

circuits_filtered_df = circuits_df.filter(circuits_df.circuit_id == 24)

# COMMAND ----------

display(circuits_filtered_df)

# COMMAND ----------

race_results_df = results_df.join(races_filtered_df, results_df.race_id == races_filtered_df.race_id, "inner") \
                            .join(circuits_filtered_df, races_filtered_df.circuit_id == circuits_filtered_df.circuit_id, "inner") \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
                            .select(races_filtered_df.race_year, races_filtered_df.race_name, races_filtered_df.race_date, circuits_filtered_df.circuit_location, drivers_df.driver_name, \
                                drivers_df.driver_number, drivers_df.driver_nationality, constructors_df.team, results_df.grid, results_df.["fastest lap"], results_df.race_time, results_df.points) \
                            

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_final_df = add_ingestion_date(race_results_df)

# COMMAND ----------

display(race_results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to the parquet file

# COMMAND ----------

race_results_final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


