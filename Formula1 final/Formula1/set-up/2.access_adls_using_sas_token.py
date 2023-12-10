# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config SAS Token
# MAGIC 1. List files from demo Container
# MAGIC 1. Read Data from circuits.csv file 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sacctformula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sacctformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sacctformula1.dfs.core.windows.net", "sp=rl&st=2023-12-10T02:03:50Z&se=2023-12-10T10:03:50Z&spr=https&sv=2022-11-02&sr=c&sig=l3EWF7zKXnJyYoqoZ%2FQ0ak1sbUbtpe2aNqHLUpJPYJk%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1dls1@sacctformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://formula1dls1@sacctformula1.dfs.core.windows.net/circuits (1).csv"))

# COMMAND ----------

