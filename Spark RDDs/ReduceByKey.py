# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('ReduceByKey')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Sample.txt')

# COMMAND ----------

# Show the result
# rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x : x.split(' '))

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x,len(x)))

# COMMAND ----------

# reduceByKey()
rdd3.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------


