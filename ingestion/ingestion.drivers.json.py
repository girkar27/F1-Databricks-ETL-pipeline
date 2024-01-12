# Databricks notebook source
dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

#using the config file values in the notebooks ussing %run magic command

# COMMAND ----------

#all colunmns are strings we we need to transform that
from pyspark.sql.types import IntegerType, StringType, DateType, DoubleType, StructField,StructType
name_schema =  StructType(fields = [StructField("forename", StringType(), True),
                            StructField("surname", StringType(), True)])

my_schema = StructType(fields = [StructField("driverId", IntegerType(),False),
                                StructField("driverRef", StringType(),True),
                                StructField("number", IntegerType(),True), 
                                StructField("code", StringType(),True),
                                StructField("name", name_schema,True), 
                                StructField("dob", DateType(), True),
                                StructField("nationality", StringType(), True),
                                StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.option("header", True).schema(my_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

new_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("driverRef", "driver_ref") \
                    .withColumn("ingestion_date",current_timestamp()) \
                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(new_df)

# COMMAND ----------

dropped_df =  new_df.drop("url")

# COMMAND ----------

display(dropped_df)

# COMMAND ----------

#writing a file in data lake container in parwuet
#here we partition the data on the bases of race year which creates a separate "folders in the dlaek containers
dropped_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
dropped_df.write.saveAsTable("f1_processed.drivers")


# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/dlakeformula1/processed/drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

