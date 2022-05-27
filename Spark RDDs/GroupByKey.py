# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('GroupByKey')
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/GroupByKey.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# Words count
# rdd.map(lambda x : (x, len(x.split(' ')))).collect() 

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

# rdd2.map(lambda x: (x,1)).collect()
data = rdd2.map(lambda x: (x,len(x)))

# COMMAND ----------

'''
    mapValues() is used to store the grouped values into a list.
    
'''
data.groupByKey().mapValues(list).collect()



# COMMAND ----------


