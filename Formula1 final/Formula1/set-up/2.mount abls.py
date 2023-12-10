# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Using Service Principal 
# MAGIC 1. Get client_id, tenant_id, client_secret from key_vault (AAD)
# MAGIC 1. Assign blob storage contributor role
# MAGIC 1. Set Spark Config with App/ Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage

# COMMAND ----------

client_id = "27065416-f8a7-487d-add0-ea27e72063f2"
tenant_id = "e24ac094-efd8-4a6b-98d5-a129b32a8c9a"
client_secret = "4E68Q~I262sKuv1051DRoKOLHlijFtGm.zemccTT"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://formula1dls1@sacctformula1.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dls1",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dls1"))

# COMMAND ----------

