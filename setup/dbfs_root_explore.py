# Databricks notebook source
# BY now we have uploaded the file circuits.csv on dbfs root and can access any data in from dbvfs blob storage
display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("FileStore/"))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/circuits.csv"))

# COMMAND ----------

