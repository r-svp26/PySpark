# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('Basics').getOrCreate()

# COMMAND ----------

df = spark.read.json('/FileStore/tables/people.json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df.describe()

# COMMAND ----------

from pyspark.sql.types import (StructField, StringType, IntegerType, StructType)

# COMMAND ----------

data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]

# COMMAND ----------

final_struct = StructType (fields=data_schema)

# COMMAND ----------

df = spark.read.json('/FileStore/tables/people.json',schema=final_struct)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df['age']
# Grab the Data Frame [Return column]

# COMMAND ----------

type(df['age'])

# COMMAND ----------

df.select('age')
# Select the Data Frame [Return the Data Frame]

# COMMAND ----------

df.select('age').show()

# COMMAND ----------

df.head(2)
# Select values from top.

# COMMAND ----------

df.head(2)[0]

# COMMAND ----------

type(df.head(2)[0])
# Info about Row object

# COMMAND ----------

df.select(['age','name'])
# Select multiple coluns


# COMMAND ----------

df.select(['age','name']).show()
# Select multiple coluns and disply the Data Frame

# COMMAND ----------

df.withColumn('double_age',df['age']*2).show()
# Add new column in the data frame

# COMMAND ----------

df.withColumnRenamed('age','my_new_age').show()
# Renaming the column name

# COMMAND ----------

df.createOrReplaceTempView('people')

# COMMAND ----------

results = spark.sql("SELECT * FROM people")

# COMMAND ----------

results.show()

# COMMAND ----------

new_results = spark.sql("SELECT * FROM people WHERE age=30")

# COMMAND ----------

new_results.show()

# COMMAND ----------


