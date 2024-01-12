# Databricks notebook source
dbutils.widgets.text("data_source", "")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------



# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

#all colunmns are strings we we need to transform that
my_hive_schema =  "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructors_df = spark.read.option("header", True).schema(my_hive_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

## add ingestion date and drop url column and rename

dropped_df = constructors_df.drop("url")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

new_df = dropped_df.withColumnRenamed("constructorsId", "constructors_id") \
                    .withColumnRenamed("constructorsRef", "constructors_ref") \
                    .withColumn("ingestion_date",current_timestamp() ) \
                    .withColumn("data_source",lit(v_data_source)) \
                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(new_df)

# COMMAND ----------

#writing a file in data lake container in parwuet
#here we partition the data on the bases of race year which creates a separate "folders in the dlaek containers
new_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
new_df.write.saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlakeformula1/processed/constructors 

# COMMAND ----------

