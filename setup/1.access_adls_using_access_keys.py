# Databricks notebook source
print("hello")

# COMMAND ----------

f1_account_key = dbutils.secrets.get(scope ="formula1-scope" , key = "formula1-account-key")

# COMMAND ----------

# access azure data lake using access keys
# list from demo coantainer
# read data from circuits.csv

# the dbutils.fs.ls would not return anything unless it is authenticated with the storage accouunt access keys

spark.conf.set("fs.azure.account.key.dlakeformula1.dfs.core.windows.net",
            f1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlakeformula1.dfs.core.windows.net"))

# COMMAND ----------

# reading csv from spark 
display(spark.read.csv("abfss://demo@dlakeformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

