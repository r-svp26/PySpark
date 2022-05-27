# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Flat Map')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Sample.txt')

# COMMAND ----------

# RDDs Data
rdd.collect()

# COMMAND ----------

mapped_rdd = rdd.map(lambda x : x.split(' '))

# COMMAND ----------

# Mapped Data 
mapped_rdd.collect()

# COMMAND ----------

flatmapped_data = rdd.flatMap(lambda x: x.split())

# COMMAND ----------

# Flat Mapped Data 
flatmapped_data.collect()

# COMMAND ----------

# MAGIC %md RDDs Filter

# COMMAND ----------

filter_data = rdd.filter( lambda x: x != '7 8 9 10')

# COMMAND ----------

# Used the filter condition
filter_data.collect()

# COMMAND ----------

# Using function
def foo(x):
    '''
        If the condition is True then It'll return all the values.
         return True
    '''
    if x != '7 8 9 10':
        return False
    else:
        return True
filtered_data = rdd.filter(foo)


# COMMAND ----------

filtered_data.collect()

# COMMAND ----------

# MAGIC %md Distinct

# COMMAND ----------

flatmapped_data = rdd.flatMap(lambda x: x.split())
flatmapped_data.collect()

# COMMAND ----------

distinct_data = flatmapped_data.distinct()

# COMMAND ----------

# Show only unique values
distinct_data.collect()

# COMMAND ----------

# Show the results in single lines.
rdd.flatMap(lambda x: x.split()).distinct().collect()

# COMMAND ----------


