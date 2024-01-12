# Databricks notebook source
# Getting file_name args into notebook for dynamic data access
dbutils.widgets.text("file_date", "")
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../config_file"

# COMMAND ----------

#starting with data ingestion circuits.csv file present in 

circuits_df = spark.read.option("header", True).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

#all colunmns are strings we we need to transform that
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

my_schema = StructType(fields=[StructField("circuitsId", IntegerType(), False), 
                               StructField("circuitRef", StringType(), True),
                               StructField("name", StringType(), True),
                               StructField("location", StringType(), False),
                               StructField("country", StringType(), False),
                               StructField("lat", DoubleType(), False),
                               StructField("lng", DoubleType(), False),
                               StructField("alt", DoubleType(), False),
                               StructField("url", StringType(), False)])



# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(my_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# using the display command of databricks to show a nice view
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit
new_renamed_df = circuits_df.withColumnRenamed("circuitsId", "circuits_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

new_renamed_df.head()

# COMMAND ----------

new_renamed_df.show()

# COMMAND ----------

#now we add a new column in the sspark data frame

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = new_renamed_df.withColumn("date", current_timestamp()).withColumn("env", lit("Production"))


# COMMAND ----------

circuits_final_df.printSchema()

# COMMAND ----------

#writing a file in data lake container in parwuet
circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.saveAsTable("f1_processed.circuits");

# COMMAND ----------

dbutils.fs.ls(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------



# COMMAND ----------

