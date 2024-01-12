-- Databricks notebook source
-- to find dominant drivers from the joined table of calculated_race_results;

-- COMMAND ----------

select * from f1_presenation.calculated_race_results;

-- COMMAND ----------

select team_name, count(1) as total_races, sum(calculated_points), avg(calculated_points) as avg_points from f1_presenation.calculated_race_results 
group by team_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------

-- to find the same results in the recent years

select team_name, count(1) as total_races, sum(calculated_points), avg(calculated_points) as avg_points from f1_presenation.calculated_race_results where race_year between 2011 and 2020 
group by team_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------

