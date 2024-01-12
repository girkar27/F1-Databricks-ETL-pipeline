# Databricks notebook source
# MAGIC %run "../config_file"
# MAGIC

# COMMAND ----------

dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date='{v_file_date}'").select("race_year").distinct().collect()
race_array = []
for race_yr in race_results_df:
    race_array.append(race_yr.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_array))

# COMMAND ----------

#for driver standings
from pyspark.sql.functions import sum, when, col, count

constructor_standings_df = race_results_df.groupBy("race_year", "constructor_name"). \
    agg(count(when(col("position") == 1, True)).alias("Wins"), sum("points").alias("total_points"))

# COMMAND ----------


final_df = constructor_standings_df.filter(constructor_standings_df.race_year== 2020)
display(final_df)

# COMMAND ----------

#put the ranks to every other driver
# using window functions

from pyspark.sql.functions import desc, rank
from pyspark.sql.window import Window

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

#adding a column of rank generated using the partion function
final_df_with_rank = final_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# final_df_with_rank.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df_with_rank.write.mode("overwrite").format("parquet").saveAsTable("f1_presenation.construction_standings")

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

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')