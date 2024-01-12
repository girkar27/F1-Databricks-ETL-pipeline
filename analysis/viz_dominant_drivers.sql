-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC header = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant F1 Driver Dashboard </h1>"""
-- MAGIC displayHTML(header)

-- COMMAND ----------


create or replace  temp view v_dominant_drivers
as
select driver_name,
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank  
from f1_presenation.calculated_race_results 
group by driver_name having total_races >= 50 order by avg_points desc;

-- COMMAND ----------



-- COMMAND ----------

-- find out the dominant drivers by grouping the year and the driver_name to find out who was dominant drivers in a particular decade scale
select race_year, driver_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) 
group by race_year, driver_name order by race_year, avg_points desc;

-- COMMAND ----------

-- see a bar chart
select race_year, driver_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) 
group by race_year, driver_name order by race_year, avg_points desc;

-- COMMAND ----------

-- see a area chart
select race_year, driver_name, 
count(1) as total_races, 
sum(calculated_points), 
avg(calculated_points) as avg_points 
from f1_presenation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10) 
group by race_year, driver_name order by race_year, avg_points desc;

-- COMMAND ----------

