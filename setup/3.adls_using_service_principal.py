# Databricks notebook source
# access azure data lake service principal: we create a new user app registration and copy the client id 
client_id = "98538ce0-7ce6-4b49-bc7c-eee0f41c204c"
tenant_id = "180242f5-30d3-49a3-af5f-1b263f3cf943"
client_secret = "1q18Q~yq.madBEEMiTyAfD3DqZgFsWawB5pJ1cVg"

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.dlakeformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlakeformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlakeformula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlakeformula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlakeformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlakeformula1.dfs.core.windows.net"))

# COMMAND ----------

# reading csv from spark 
display(spark.read.csv("abfss://demo@dlakeformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

