# Databricks notebook source
# access azure data lake service principal: we create a new user app registration and copy the client id 
client_id = "98538ce0-7ce6-4b49-bc7c-eee0f41c204c"
tenant_id = "180242f5-30d3-49a3-af5f-1b263f3cf943"
client_secret = "1q18Q~yq.madBEEMiTyAfD3DqZgFsWawB5pJ1cVg"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#mounting adls
# abfss://<container_name>@<data lake name>.dfs.windows.net
mount_point = "/nmt/<any structure>"
dbutils.fs.mount(
    source = "abfss://demo@dlakeformula1.dfs.core.windows.net",
    mount_point = "/mnt/dlakeformula1/demo/",
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dlakeformula1/demo/"))

# COMMAND ----------

# reading csv from spark 
display(spark.read.csv("/mnt/dlakeformula1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

