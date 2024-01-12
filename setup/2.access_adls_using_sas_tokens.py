# Databricks notebook source
#getting the variable using get method from key vault
# dbutils.secrets.help()
# dbutils.secrets.listScopes()
f1_sas_token = dbutils.secrets.get(scope ="formula1-scope" , key = "f1-demo-key-sas-token")

# COMMAND ----------

#Auth for Sas token

# the dbutils.fs.ls would not return anything unless it is authenticated with the storage accouunt access keys

spark.conf.set("fs.azure.account.key.dlakeformula1.dfs.core.windows.net",
            f1_sas_token) 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlakeformula1.dfs.core.windows.net"))

# COMMAND ----------

# reading csv from spark 
display(spark.read.csv("abfss://demo@dlakeformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

