# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName('Read File')

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Sample.txt')

# COMMAND ----------

# Display the data
rdd.collect()

# COMMAND ----------

# Count the records
rdd.count()

# COMMAND ----------

# MAGIC %md RDDs map()

# COMMAND ----------

# Split the x values
'''
    Using String Concatanation
'''
# maped_data = rdd.map(lambda x: x + " Ritesh")
'''
    Using split() 
'''
maped_data = rdd.map(lambda x: x.split(' '))

# COMMAND ----------

# Show the data
# maped_data.collect()

# COMMAND ----------

def splitNumber(x):
    return x.split(' ')

result = rdd.map(splitNumber)

# COMMAND ----------

# Show the result
result.collect()

# COMMAND ----------

def foo(x):
   l = x.split(' ') 
   l2 = []
   for s in l:
    l2.append(int(s) + 10)
    return l2

final_result = result.map(foo)
        

# COMMAND ----------

result.collect()

# COMMAND ----------


