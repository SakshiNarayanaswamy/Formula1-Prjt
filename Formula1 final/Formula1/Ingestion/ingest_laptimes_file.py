# Databricks notebook source
# MAGIC %md
# MAGIC Read csv file(here it is multiple csv)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
laptimes_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)

]) 

# COMMAND ----------

laptimes_df = spark.read.schema(laptimes_schema).csv(f"dbfs:/FileStore/tables/{v_file_date}/lap_times/")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC Rename and Add columns

# COMMAND ----------

from pyspark.sql.functions import col , concat, current_timestamp, lit
laptimes_final_df = laptimes_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(laptimes_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Write the output to datalake

# COMMAND ----------

#laptimes_final_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/lap_times")
#laptimes_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

merge_delta_data(laptimes_final_df, 'f1_processed','lap_times', "dbfs:/FileStore/processed", "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap", 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

