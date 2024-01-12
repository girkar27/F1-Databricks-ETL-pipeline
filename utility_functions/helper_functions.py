# Databricks notebook source
# MAGIC %run "../config_file"

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column, v_file_date, date):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if v_file_date == date:
    output_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/{table_name}")
    output_df.write.mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("append").parquet(f"{presentation_folder_path}/{table_name}")
    output_df.write.mode("append").saveAsTable(f"{db_name}.{table_name}")
    # output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

