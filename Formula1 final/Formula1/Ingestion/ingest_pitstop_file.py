# Databricks notebook source
# MAGIC %md
# MAGIC Read json file(here it is multiline json)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
pitstops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("stop",StringType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("duration",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)

]) 

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).option("multiLine",True).json(f"dbfs:/FileStore/tables/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Rename and Add columns

# COMMAND ----------

from pyspark.sql.functions import col , concat, current_timestamp, lit
pitstops_with_columns_df = pitstops_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pitstops_with_columns_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Write the output to datalake

# COMMAND ----------

#pitstops_with_columns_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/pit_stops")
#pitstops_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

merge_delta_data(pitstops_with_columns_df, 'f1_processed','pit_stops', "dbfs:/FileStore/processed", "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id", 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

