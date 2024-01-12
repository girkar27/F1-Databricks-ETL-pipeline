# Databricks notebook source
print("hello")

# COMMAND ----------

#cluster Scope crednetials
# set spark config at cluster level
#hence the auth keys are saved at a cluster level hence we do not need set configurations to every notebooks....

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlakeformula1.dfs.core.windows.net"))

# COMMAND ----------

# reading csv from spark 
display(spark.read.csv("abfss://demo@dlakeformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

