-- Databricks notebook source
-- MAGIC %md
-- MAGIC created presenation containers

-- COMMAND ----------

create database if not exists f1_presenation
location "/mnt/dlakeformula1/presentation";

-- COMMAND ----------

desc database f1_presenation;

-- COMMAND ----------

--using ingestion notebooks to create database tables as well as write the files

-- COMMAND ----------

use database f1_presenation;

-- COMMAND ----------

drop table construction_standings;

-- COMMAND ----------

