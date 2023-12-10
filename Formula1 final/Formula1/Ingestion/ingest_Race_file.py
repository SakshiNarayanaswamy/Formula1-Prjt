# Databricks notebook source
from pyspark.sql.types import StructType, StructField , IntegerType, StringType, DoubleType, DateType 

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Define the structure type

# COMMAND ----------

Race_schema = StructType(fields=[
    StructField("raceId", IntegerType() , False),
    StructField("year", IntegerType() , True),
    StructField("round", IntegerType() , True),
    StructField("circuitId", IntegerType() , True),
    StructField("name", StringType() , True),
    StructField("date", DateType()  , True),
    StructField("time", StringType() , True),
    StructField("url", StringType() , True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dataframe

# COMMAND ----------

print(v_file_date)

# COMMAND ----------

race_df = spark.read.option("header",True).schema(Race_schema).csv(f"dbfs:/FileStore/tables/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Display Dataframe

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC Select required columns

# COMMAND ----------

from pyspark.sql.functions import col
race_selected_df = race_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename Columns

# COMMAND ----------

race_renamed_df = race_selected_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

display(race_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Concat date and Time to timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit  , to_timestamp , concat, col, lit
race_new_df = race_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))
race_final_df = race_new_df.withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

from pyspark.sql.functions import col
race_final_df = race_final_df.select(col("race_Id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"),col("file_date"))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the output to processed container in parquet format

# COMMAND ----------

#race_final_df.write.mode("overwrite").partitionBy('race_year').parquet("dbfs:/FileStore/processed/races")
#race_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.race_final")
race_final_df.write.mode("append").partitionBy('race_year').format("delta").saveAsTable("f1_processed.race_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.race_final WHERE file_date = '2021-04-18';

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/processed/race_final

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/processed/race_final'))

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/processed/race_final'))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

