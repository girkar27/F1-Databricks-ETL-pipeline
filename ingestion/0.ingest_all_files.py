# Databricks notebook source
#running Databricks notebook workflows
data_source = dbutils.notebook.run("ingestion.constructors.json", 0, {"data_source": "EARGAST_API"})

# COMMAND ----------

