# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('missdata').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/ContainsNull.csv',inferSchema=True,header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

# Drop the missing data
df.na.drop().show()

# COMMAND ----------

# Used thresh means row contains min two non-value 
df.na.drop(thresh=2).show()

# COMMAND ----------

# all or any in how
'''
    Drop all the null values rows.
'''
df.na.drop(how='any').show()

# COMMAND ----------

# Only drop the rows if all the values are null.
df.na.drop(how='all').show()

# COMMAND ----------

# Using subset - To drop by specific column
df.na.drop(subset=['Sales']).show()

# COMMAND ----------

# By using name where name is null
df.na.drop(subset=['Name']).show()

# COMMAND ----------

# Filling the missing values

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Define subset
df.na.fill('No Name',subset=['Name']).show()

# COMMAND ----------

# For numeric value
df.na.fill(0,subset=['Sales']).show()

# COMMAND ----------

# Mean values

# COMMAND ----------

from pyspark.sql.functions import mean

# COMMAND ----------

mean_value = df.select(mean(df['Sales'])).collect()

# COMMAND ----------

# Display mean value
mean_value

# COMMAND ----------

# Display only value
mean_sales = mean_value[0][0]

# COMMAND ----------

# Fill the mean value to null in data frame.

df.na.fill(mean_sales,['Sales']).show()

# COMMAND ----------

# Single line command
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0],['Sales']).show()

# COMMAND ----------


