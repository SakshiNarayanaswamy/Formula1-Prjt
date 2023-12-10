# Databricks notebook source
# MAGIC %md 
# MAGIC Produce driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')
race_year_list

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum , when , count, col

driver_standings_df  = race_results_df \
    .groupBy("race_year","driver_name","driver_nationality") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))
    

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
#overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

merge_delta_data(final_df, 'f1_presentation','driver_standings', "dbfs:/FileStore/presentation", "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year", 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings ;

# COMMAND ----------

