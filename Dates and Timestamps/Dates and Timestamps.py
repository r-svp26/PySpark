# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('date').getOrCreate()

# COMMAND ----------

df =spark.read.csv('/FileStore/tables/appl_stock.csv',inferSchema=True,header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.head(1)

# COMMAND ----------

df.select(['Date','Open','Close']).show()

# COMMAND ----------

from pyspark.sql.functions import (dayofmonth,hour,dayofyear,
                                   month,year,weekofyear, 
                                   format_number,date_format)

# COMMAND ----------

# Show the date
df.select(dayofmonth(df['Date'])).show()

# COMMAND ----------

# Show the hour
df.select(hour(df['Date'])).show()

# COMMAND ----------

# Show the month
df.select(month(df['Date'])).show()

# COMMAND ----------

# Year
df.select(year('Date')).show()

# COMMAND ----------

# Add new column Year 
newdf = df.withColumn("Year",year('Date')).show()

# COMMAND ----------

# Store the result
newdf = df.withColumn("Year",year('Date'))

# COMMAND ----------

# Average closing price per year
newdf.groupBy("Year").mean().show()

# COMMAND ----------

# Show average 
newdf.groupBy("Year").mean().select(["Year","avg(Close)"]).show()

# COMMAND ----------

# Store the value in result
result = newdf.groupBy("Year").mean().select(["Year","avg(Close)"])

# COMMAND ----------

'''
    Average Closing price per year 
'''
data = result.withColumnRenamed("avg(Close)","Average Closing Price").show()

# COMMAND ----------

data = result.withColumnRenamed("avg(Close)","Average Closing Price")

# COMMAND ----------

# Format the data upto decimal points.
data.select(['Year',format_number("Average Closing Price",2).alias("Average Closing Price")]).show()

# COMMAND ----------


