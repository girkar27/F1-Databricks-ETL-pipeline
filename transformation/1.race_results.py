# Databricks notebook source
# MAGIC %md
# MAGIC #The races, constructors, curcuits, drivers data is of full load so we do not need to change that
# MAGIC #the changes will come to the results df where we will need to add a filter

# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

#reading data from the dlake containers
races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df =  spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df =  spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "constructor_name")

# COMMAND ----------

results_df =  spark.read.parquet(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "result_time") \
        .withColumnRenamed("race_id", "result_race_id") \
            .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

#joining circuit_race_df and selecting feilds similar to BBC
circuit_races_df = circuits_df.join(races_df, circuits_df.circuits_id ==  races_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_name, races_df.race_year, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------



# COMMAND ----------

race_results_df = results_df.join(drivers_df, drivers_df.driver_id ==  results_df.driver_id) \
    .join(circuit_races_df, results_df.result_race_id == circuit_races_df.race_id) \
        .join(constructors_df, constructors_df.constructorId == results_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "constructor_name", "grid", "fastest_lap", "result_time", "points", "position", "result_file_date").withColumn("created_date", current_timestamp()).withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

#creating a Display levvel data frame by filtering for  2020 abu dhabhi Grand Prix

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if v_file_date == "2021-03-21":
    output_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/{table_name}")
    output_df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("append").parquet(f"{presentation_folder_path}/{table_name}")
    output_df.write.mode("append").saveAsTable(f"{db_name}.{table_name}")
    # output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

#writing the entire data of final df to race_results
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")
# final_df.write.mode("append").format("parquet").saveAsTable("f1_presenation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_presentation.race_results group by race_id order by race_id desc;

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)