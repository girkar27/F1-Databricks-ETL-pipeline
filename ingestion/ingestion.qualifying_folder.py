# Databricks notebook source
# MAGIC %run "../config_file"

# COMMAND ----------

dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

#all colunmns are strings we we need to transform that
#Reading multiple json files through qualifying folder
from pyspark.sql.types import IntegerType, StringType, DateType, DoubleType, StructField,StructType, FloatType
my_schema = StructType(fields = [StructField("qualifyId", IntegerType(),False),
                                StructField("raceId", IntegerType(),True),
                                StructField("driverId", IntegerType(),True),
                                StructField("constructorId", IntegerType(),True),
                                StructField("number", IntegerType(),True), 
                                StructField("position", IntegerType(),True),
                                StructField("q1", StringType(),True),
                                StructField("q2", StringType(),True), 
                                StructField("q3", StringType(), True)])

# COMMAND ----------

##reading multiline json using the option function 
results_df = spark.read.option("header", True).option("multiLine", True).schema(my_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split_*.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

new_df = results_df.withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                         .withColumnRenamed("qualifyId", "qualify_id") \
                             .withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumn("ingestion_date",current_timestamp()) \
                                                .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(new_df)

# COMMAND ----------

#writing a file in data lake container in parwuet
#here we partition the data on the bases of race year which creates a separate "folders in the dlaek containers
if v_file_date == "2021-03-21":
    mode = "overwrite"
    spark.sql("drop table if exists f1_processed.qualifying")
else:
    mode = "append"
new_df.write.mode(mode).parquet(f"{processed_folder_path}/qualifying")
new_df.write.mode(mode).saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/dlakeformula1/processed/qualifying
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.qualifying group by race_id order by race_id desc;

# COMMAND ----------

