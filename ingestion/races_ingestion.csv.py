# Databricks notebook source
dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

#all colunmns are strings we we need to transform that
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
my_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                               StructField("year", IntegerType(), True),
                               StructField("round", IntegerType(), True),
                               StructField("circuitId", IntegerType(), True),
                               StructField("name", StringType(), True),
                               StructField("date", DateType(), True),
                               StructField("time", StringType(), True),
                               StructField("url", StringType(), True)])



# COMMAND ----------

race_df = spark.read.option("header", True).schema(my_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

race_df.show()

# COMMAND ----------

## add ingestion date and race_timestamp to data frame
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
race_df_with_timestamp = race_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

race_selected_df = race_df_with_timestamp.select(col('raceId').alias("race_id"), col('year').alias("race_year"), col('round'), col('circuitId').alias("circuit_id"), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

#writing a file in data lake container in parwuet
#here we partition the data on the bases of race year which creates a separate folders in the dlaek containers
race_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
race_selected_df.write.saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/dlakeformula1/processed/races 

# COMMAND ----------

