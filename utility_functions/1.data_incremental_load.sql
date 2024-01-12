-- Databricks notebook source
-- MAGIC %md
-- MAGIC Drop all the full load tables

-- COMMAND ----------

drop database if exists f1_processed cascade;
drop database if exists f1_presenation cascade; 

-- COMMAND ----------

create database f1_processed
location "mnt/dlakeformula1/processed";

-- COMMAND ----------

create database f1_presentation
location "mnt/dlakeformula1/processed";

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from circuits;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from circuits;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table results;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table lap_times;

-- COMMAND ----------

show tables;

-- COMMAND ----------

use database f1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table race_results;

-- COMMAND ----------

show tables;

-- COMMAND ----------

