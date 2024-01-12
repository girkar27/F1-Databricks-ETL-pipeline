-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC header = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant F1 Constructor Dashboard </h1>"""
-- MAGIC displayHTML(header)

-- COMMAND ----------


create or replace  temp view v_dominant_drivers
as
select team_name,
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank  
from f1_presenation.calculated_race_results 
group by team_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------



-- COMMAND ----------

-- find out the dominant drivers by grouping the year and the driver_name to find out who was dominant drivers in a particular decade scale
select race_year, team_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where team_name in (select team_name from v_dominant_drivers where team_rank <= 10) 
group by race_year, team_name order by race_year, avg_points desc;

-- COMMAND ----------

-- see a bar chart
select race_year, team_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where team_name in (select team_name from v_dominant_drivers where team_rank <= 10) 
group by race_year, team_name order by race_year, avg_points desc;

-- COMMAND ----------

-- see a area chart
select race_year, team_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where team_name in (select team_name from v_dominant_drivers where team_rank <= 10) 
group by race_year, team_name order by race_year, avg_points desc;

-- COMMAND ----------

