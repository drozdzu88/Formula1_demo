# Databricks notebook source
# MAGIC %md
# MAGIC ###Produce constructor standings

# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count
constructor_standings_df = race_results_df.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), 
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructors_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


