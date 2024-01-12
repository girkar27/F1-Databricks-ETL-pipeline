-- Databricks notebook source
--calculating the dominant drivers overall in sql

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create table f1_presenation.calculated_race_results 
using parquet as select races.race_year, constructors.name as team_name, drivers.name as driver_name, results.position, results.points, (11 - results.position) as calculated_points from results 
      join drivers on (results.driver_id = drivers.driver_id)
      join constructors on (constructors.constructorId =  results.constructor_id)
      join races on (races.race_id = results.race_id) where results.position <= 10; 


-- COMMAND ----------

select * from f1_presenation.calculated_race_results;

-- COMMAND ----------

