# Databricks notebook source
# MAGIC %md
# MAGIC Read the json file using the spark dataframe reader api

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)])
drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)

])                                 

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"dbfs:/FileStore/tables/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col , concat, current_timestamp, lit
drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("ingestion_date", current_timestamp()).withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the unwanted column

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/drivers")
#drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet("dbfs:/FileStore/processed/drivers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

