# Databricks notebook source
# MAGIC %md
# MAGIC First went to admin console by clicking in the mail id(top right corner of the screen)
# MAGIC
# MAGIC Then navigated to DBFS file browser and enabled in the settings
# MAGIC
# MAGIC Then navigated to Catalog and --> Browse DBFS --> Tables ---> Uploaded
# MAGIC
# MAGIC Then Came back to this notebook to see if all the files were added as expected and was able to understand how to access these

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/tables/circuits.csv'))

# COMMAND ----------

