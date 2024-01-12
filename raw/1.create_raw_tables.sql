-- Databricks notebook source
-- MAGIC %md
-- MAGIC # creating External Tables (importing raw data manually from Eargast APi Hence we will data lakke data in out control)

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId INT,
  circuitRef string, 
  name string,
  location string, 
  country string,
  lat double, 
  lng double, 
  alt int, 
  url string 
) using csv
options(path "/mnt/dlakeformula1/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId INT,
  year int, 
  round int,
  circuitId int, 
  name string,
  date date, 
  time string, 
  url string  
) using csv
options (path "/mnt/dlakeformula1/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##writing json files to database

-- COMMAND ----------

-- writing constructors.json file to database
drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
) using json
options (path "/mnt/dlakeformula1/raw/constructors.json", header true)


-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- writing drivers.json file to database (conmplex structure single line json)
create table if not exists f1_raw.drivers(
  driverId INT, 
  driverRef STRING, 
  number int, 
  code STRING, 
  name STRUCT<forename: STRING, surname: STRING>, 
  dob date,
  nationality string, 
  url string 
) using json
options (path "/mnt/dlakeformula1/raw/drivers.json", header true)


-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

 drop table if exists f1_raw.results;
 create table if not exists f1_raw.results(
  constructorId int,
  driverId int,
  fastestLap int, 
  fastestLapSpeed float, 
  fastestLaptime float, 
  grid int, 
  laps int, 
  points double, 
  position int, 
  positionOrder int, 
  positionText string, 
  raceId  int,
  rank int, 
  resultId int, 
  statusId int, 
  time string
 ) using json
 options (path "/mnt/dlakeformula1/raw/results.json", header true);

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

--creating pitstops file multiline json
drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId int, 
  duration STRING, 
  lap int, 
  milliseconds int, 
  raceId int, 
  stop int,
  time string 
) using json
options (path "/mnt/dlakeformula1/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # creating tables from folder containing set of similar files

-- COMMAND ----------

--creating pitstops file multiline json
drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int, 
  driverId int, 
  lap int, 
  position int,
  time string,
  milliseconds int 
) using csv
options (path "/mnt/dlakeformula1/raw/lap_times");

-- COMMAND ----------

-- for qualifying folder
--creating pitstops file multiline json
drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  constructorId int, 
  driverId int, 
  number int, 
  position int,
  q1 string,
  q2 string,
  q3 string,
  qualifyId int, 
  raceId int 
) using json
options (path "/mnt/dlakeformula1/raw/qualifying", multiLine true);

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from f1_processed.circuits;

-- COMMAND ----------

