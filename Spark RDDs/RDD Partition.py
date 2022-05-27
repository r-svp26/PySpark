#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Partition')
sc = SparkContext.getOrCreate(conf=conf)


# In[20]:


rdd = sc.textFile('D:\Pyspark Training\PySpark\Spark RDDs\Word Count Quiz.txt')
rdd = rdd.repartition(5)
rdd2 = rdd2.flatMap(lambda x : x.split(''))
rdd3 = rdd2.map(lambda x :(x,len(x)))


# In[12]:


# rdd.collect()
rdd3.getNumPartitions()


# In[26]:


# rdd3.saveAsTextFile('D:Pyspark/Training/PySpark/Spark RDDs/Output/partition')


# # coalesce()

# In[29]:


rdd = sc.textFile('D:\Pyspark Training\PySpark\Spark RDDs\Word Count Quiz.txt')
rdd = rdd.repartition(5)
rdd2 = rdd2.flatMap(lambda x : x.split(''))
rdd3 = rdd2.map(lambda x :(x,len(x)))
rdd4 = rdd.repartition(2)


# In[30]:


rdd4.getNumPartitions()


# In[ ]:




