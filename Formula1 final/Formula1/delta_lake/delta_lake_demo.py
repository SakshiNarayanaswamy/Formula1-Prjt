# Databricks notebook source
# MAGIC %md
# MAGIC ###### 1. Write data to delta lake(managed table)
# MAGIC ###### 2. Write data to delta lake(external table)
# MAGIC ###### 3. Read data from delta lake (Table)
# MAGIC ###### 4. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_delta
# MAGIC LOCATION 'dbfs:/FileStore/delta'

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema",True) \
    .json("dbfs:/FileStore/tables/2021-03-28/results.json")    

# COMMAND ----------

 results_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

 results_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/delta/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/delta/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("dbfs:/FileStore/delta/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

 results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_delta.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_delta.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC Update Delta Table
# MAGIC
# MAGIC Delete from Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC #####Update In SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  f1_delta.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Update In Python

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, "dbfs:/FileStore/delta/results_managed")
deltaTable.update("position <= 10" ,{ "points":"21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Delete In SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_delta.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Delete In Python

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, "dbfs:/FileStore/delta/results_managed")
deltaTable.delete("points = 0" )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read\
    .option("inferSchema",True) \
    .json("dbfs:/FileStore/tables/2021-03-28/drivers.json")\
    .filter("driverId <= 10")\
    .select("driverId", "dob", "name.forename" , "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read\
    .option("inferSchema",True) \
    .json("dbfs:/FileStore/tables/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 6 AND 15")\
    .select("driverId", "dob", upper("name.forename").alias("forename") , upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read\
    .option("inferSchema",True) \
    .json("dbfs:/FileStore/tables/2021-03-28/drivers.json")\
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
    .select("driverId", "dob", upper("name.forename").alias("forename") , upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Upsert in SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE  SET tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE  SET tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day3 (IN PYTHON)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/FileStore/delta/drivers_merge")
deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
    .whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"}) \
    .whenNotMatchedInsert(values = 
     {
         "driverId": "upd.driverId",
         "dob" : "upd.dob",
         "forename" : "upd.forename",
         "surname" : "upd.surname",
         "createdDate" : "current_timestamp()"
     }                     
                          )\
                              .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC
# MAGIC 2. Time Travel
# MAGIC
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_delta.drivers_merge TIMESTAMP AS OF '2023-12-06T08:46:08.000+0000';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2023-12-06T08:46:08.000+0000').load("dbfs:/FileStore/delta/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM f1_delta.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_delta.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING f1_delta.drivers_merge VERSION AS OF 10 src
# MAGIC ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  TABLE IF NOT EXISTS f1_delta.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_delta.drivers_txn
# MAGIC SELECT * FROM f1_delta.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_delta.drivers_txn
# MAGIC SELECT * FROM f1_delta.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table IF NOT EXISTS f1_delta.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_delta.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_delta.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_delta.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_delta.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("dbfs:/FileStore/delta/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/FileStore/delta/drivers_convert_to_delta_new`
# MAGIC

# COMMAND ----------

