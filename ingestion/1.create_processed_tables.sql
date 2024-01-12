-- Databricks notebook source
-- MAGIC %md
-- MAGIC #create a managed table in the processed location (default managed table creation location /hive/warehouse/<table_name>)

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/dlakeformula1/processed";

-- COMMAND ----------

-- given below is the default db location
desc database f1_raw;

-- COMMAND ----------

desc database f1_processed;

-- COMMAND ----------

use database f1_processed;

-- COMMAND ----------

