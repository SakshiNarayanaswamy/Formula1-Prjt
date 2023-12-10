# Databricks notebook source
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(spark.read.csv('/FileStore/tables/circuits.csv'))

# COMMAND ----------

