# Databricks notebook source


# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

# TO handle incremental load analyze the data in each of the dat folders in raw container
spark.read.json(f"{raw_folder_path}/2021-03-21/results.json").createOrReplaceTempView("results_cutover");
spark.read.json(f"{raw_folder_path}/2021-03-28/results.json").createOrReplaceTempView("results_next1");
spark.read.json(f"{raw_folder_path}/2021-04-18/results.json").createOrReplaceTempView("results_next2");

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1) from results_cutover group by raceId order by raceId desc;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- checking other date folder raceids
# MAGIC select raceId, count(1) from results_next1 group by raceId order by raceId desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1) from results_next2 group by raceId order by raceId desc;

# COMMAND ----------

# MAGIC %md
# MAGIC As the load is incremental we use the incremental load design to load the data in race results
# MAGIC

# COMMAND ----------

dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

#all colunmns are strings we we need to transform that
from pyspark.sql.types import IntegerType, StringType, DateType, DoubleType, StructField,StructType, FloatType
my_schema = StructType(fields = [StructField("constructorId", IntegerType(),False),
                                StructField("driverId", IntegerType(),True),
                                StructField("fastestLap", IntegerType(),True), 
                                StructField("fastestLapSpeed", FloatType(),True),
                                StructField("fastestLapTime", StringType(),True),
                                StructField("grid", IntegerType(),True), 
                                StructField("laps", IntegerType(), True),
                                StructField("points", FloatType(), True),
                                StructField("position", IntegerType(), True),
                                StructField("positionOrder", IntegerType(), True),
                                StructField("positionText", StringType(), True),
                                StructField("raceId", IntegerType(), True),
                                StructField("rank", IntegerType(), True),
                                StructField("resultId", IntegerType(), True),
                                StructField("statusId", StringType(), True),
                                StructField("time", StringType(), True)])

# COMMAND ----------

results_df = spark.read.option("header", True).schema(my_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

new_df = results_df.withColumnRenamed("constructorId", "constructor_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("fastestlap", "fastest_lap") \
                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                     .withColumnRenamed("positionText", "position_text") \
                                         .withColumnRenamed("raceId", "race_id") \
                                             .withColumnRenamed("resultId", "result_id") \
                                                .withColumnRenamed("statusId", "status_id") \
                                                .withColumn("ingestion_date",current_timestamp()) \
                                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(new_df)

# COMMAND ----------

dropped_df =  new_df.drop("status_id")

# COMMAND ----------

# for error handling of incremental load
# for rr in dropped_df.select("race_id").distinct().collect():
#     spark.sql(f"alter table f1_processed.results drop if exists partition ( race_id = {rr.race_id})")

# COMMAND ----------

#writing a file in data lake container in parwuet
#here we partition the data on the bases of race year which creates a separate "folders in the dlaek containers
if v_file_date == "2021-03-21":
    mode = "overwrite"
    spark.sql("drop table if exists f1_processed.results")
else:
    mode = "append"
dropped_df.write.mode(mode).partitionBy("race_id").parquet(f"{processed_folder_path}/results")
dropped_df.write.mode(mode).saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/dlakeformula1/processed/results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.results group by race_id order by race_id desc;

# COMMAND ----------

