# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('aggs').getOrCreate()

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/sales_info.csv',inferSchema=True,header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

# Print the Schema name
df.printSchema()

# COMMAND ----------

# Using groupby 
df.groupBy("Company")

# COMMAND ----------

df.groupBy("Company").mean().show()

# COMMAND ----------

# Sum of all the sales of a company
df.groupBy("Company").sum().show()

# COMMAND ----------

# Max sales of a company
df.groupBy("Company").max().show()

# COMMAND ----------

# Min sales of a company
df.groupBy("Company").min().show()

# COMMAND ----------

# Count no of employees in a companies
df.groupBy("Company").count().show()

# COMMAND ----------

# Aggregate function using dictionary
df.agg({'Sales':'sum'}).show()

# COMMAND ----------

# Max Sales of company
df.agg({'Sales':'max'}).show()

# COMMAND ----------

# Min Sales of company
df.agg({'Sales':'min'}).show()

# COMMAND ----------

# Mean Sales of company
df.agg({'Sales':'min'}).show()

# COMMAND ----------

# Group the data 
group_data = df.groupBy("Company")

# COMMAND ----------

# Groubyby and Aggregate function using dictionary 
group_data.agg({'Sales':'max'}).show()

# COMMAND ----------

# Some PySpark functions

# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg, stddev 

# COMMAND ----------

# Average function
df.select(avg('Sales').alias('Average Sales')).show()

# COMMAND ----------

# Standard deviation 
df.select(stddev('Sales')).show()

# COMMAND ----------

from pyspark.sql.functions import format_number

# COMMAND ----------

# Standard deviation formating 
df.select(stddev('Sales')).show()

# COMMAND ----------

std_sales = df.select(stddev('Sales').alias('std'))

# COMMAND ----------

# Number format upto 2 decimal value
std_sales.select(format_number('std',2).alias('std')).show()

# COMMAND ----------

df.show()

# COMMAND ----------

# OrderBy
df.orderBy("Sales").show()

# COMMAND ----------

# Desending order 
df.orderBy(df['Sales'].desc()).show()

# COMMAND ----------


