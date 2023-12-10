# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
qualifying_schema = StructType(fields=[StructField("qualifyingId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("q1",StringType(),True),
                                    StructField("q2",StringType(),True),
                                    StructField("q3",StringType(),True)

])    

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine",True).json(f"dbfs:/FileStore/tables/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col , concat, current_timestamp, lit
qualifying_with_columns_df = qualifying_df.withColumnRenamed("qualifyingId","qualifying_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(qualifying_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC drop column 

# COMMAND ----------

# MAGIC %md
# MAGIC Write the output to datalake

# COMMAND ----------

#qualifying_with_columns_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/qualifying")
#qualifying_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")


# COMMAND ----------

merge_delta_data(qualifying_with_columns_df, 'f1_processed','qualifying', "dbfs:/FileStore/processed", "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id", 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

