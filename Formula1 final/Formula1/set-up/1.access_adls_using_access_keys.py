# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo Container
# MAGIC 1. Read Data from circuits.csv file 

# COMMAND ----------

spark.conf.set("fs.azure.account.key.sacctformula1.dfs.core.windows.net","HEGy17nnL3ww5ZsS9j7SfJSYMjzoPbQBNCfUrSU2Lr2ZW8WDP/xSfpulYoFTO2HNBXsHpsR3V4vR+ASt6InxYw==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1dls1@sacctformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://formula1dls1@sacctformula1.dfs.core.windows.net/circuits (1).csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dls1/raw1

# COMMAND ----------

