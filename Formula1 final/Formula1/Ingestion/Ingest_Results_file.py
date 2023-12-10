# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("grid",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("positionText",StringType(),True),
                                    StructField("positionOrder",IntegerType(),True),
                                    StructField("points",FloatType(),True),
                                    StructField("laps",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True),
                                    StructField("fastestLap",IntegerType(),True),
                                    StructField("rank",IntegerType(),True),
                                    StructField("fastestLapTime",StringType(),True),
                                    StructField("fastestLapSpeed",StringType(),True),
                                    StructField("statusId",IntegerType(),True)

])    

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"dbfs:/FileStore/tables/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col , concat, current_timestamp, lit
results_with_columns_df = results_df.withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("FastestLapSpeed","fastest_lap_speed").withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC drop column 

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate remove from dataframe

# COMMAND ----------

results_dedupl_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Write the output to datalake

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"AlTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("dbfs:/FileStore/processed/results")
# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method2

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed','results','race_id')


# COMMAND ----------

merge_delta_data(results_dedupl_df, 'f1_processed','results', "dbfs:/FileStore/processed", "tgt.result_id = src.result_id AND tgt.race_id = src.race_id", 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

