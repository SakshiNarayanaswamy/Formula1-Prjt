# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_processed;

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(v_file_date)

# COMMAND ----------

drivers_df = spark.read.format("delta").load("dbfs:/FileStore/processed/drivers").withColumnRenamed("number","driver_number").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load("dbfs:/FileStore/processed/constructors").withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load("dbfs:/FileStore/processed/circuits").withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load("dbfs:/FileStore/processed/race_final").withColumnRenamed("name","race_name").withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

results_df = spark.read.format("delta").load("dbfs:/FileStore/processed/results").filter(f"file_date == '{v_file_date}'").withColumnRenamed("time","race_time").withColumnRenamed("file_date","result_file_date").withColumnRenamed("race_id","result_race_id")

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner").select(races_df.race_Id, races_df.race_year, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

races_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_Id).join(drivers_df, results_df.driver_id == drivers_df.driver_id).join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = races_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location","driver_name", "driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date").withColumn("created_date",current_timestamp()).withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("dbfs:/FileStore/presentation/race_results")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
#overwrite_partition(final_df, 'f1_presentation' , 'race_results', 'race_id')

# COMMAND ----------

merge_delta_data(final_df, 'f1_presentation','race_results', "dbfs:/FileStore/presentation", "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name", 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results WHERE race_year = 2021;

# COMMAND ----------

