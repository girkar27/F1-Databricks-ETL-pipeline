-- Databricks notebook source
-- to find dominant drivers from the joined table of calculated_race_results;

-- COMMAND ----------

select driver_name, count(1) as total_races, sum(calculated_points), avg(calculated_points) as avg_points from f1_presenation.calculated_race_results 
group by driver_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------

-- to find the same results in the recent years

select driver_name, count(1) as total_races, sum(calculated_points), avg(calculated_points) as avg_points from f1_presenation.calculated_race_results where race_year between 2011 and 2020 
group by driver_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------

