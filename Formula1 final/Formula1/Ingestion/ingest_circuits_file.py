# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Step-1 Read the CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/formula1dls1/raw1

# COMMAND ----------

from pyspark.sql.types import StructType, StructField , IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType() , False),
    StructField("circuitRef", StringType() , True),
    StructField("name", StringType() , True),
    StructField("location", StringType() , True),
    StructField("country", StringType() , True),
    StructField("lat", DoubleType() , True),
    StructField("lng", DoubleType() , True),
    StructField("alt", IntegerType() , True),
    StructField("url", StringType() , True)
])

# COMMAND ----------

#To use automatic schema developed by the spark accrd to data then use below code
# circuits_df = spark.read.option("header",True).option("inferSchema", True).csv("dbfs:/mnt/formula1dls1/raw1/circuits.csv")
# To use own schema developed by you use below code
circuits_df = spark.read.option("header",True).schema(circuits_schema).csv(f"dbfs:/FileStore/tables/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select only required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = add_ingestion_date(circuits_renamed_df)
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
circuits_add_literalcol_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()).withColumn("env", lit("Production"))

# COMMAND ----------

display(circuits_final_df)
display(circuits_add_literalcol_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Data to datalake as parquet

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/circuits")

#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/processed/circuits

# COMMAND ----------

#df = spark.read.parquet("dbfs:/FileStore/processed/circuits")

# COMMAND ----------

#display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

#delete files from folder
#dbutils.fs.rm('dbfs:/FileStore/processed/results', True)

# COMMAND ----------

