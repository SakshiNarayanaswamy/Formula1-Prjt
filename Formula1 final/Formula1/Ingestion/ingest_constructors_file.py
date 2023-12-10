# Databricks notebook source
# MAGIC %md
# MAGIC Read json file

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING , name STRING ,nationality STRING , url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"dbfs:/FileStore/tables/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC Rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("ingestion_date",current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Write output to parquet file

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("dbfs:/FileStore/processed/constructors")
#constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/processed/constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

