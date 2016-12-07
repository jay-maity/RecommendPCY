""" Generation of Large test dataset"""

import sys,os
from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark_cassandra

keyspace_input=sys.argv[1]
output=sys.argv[2]
#order=sys.argv[3:]
#orders=",".join(order)

def df_for(keyspace, table, split_size=1500):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.registerTempTable(table)
    return df

def func_topair(v):
    x=str(v.orderkey)
    z=str(v.partkey)
    return x,z

cluster_seeds = ['199.60.17.136', '199.60.17.173']
conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

odf=df_for(keyspace_input,'orders') 
ldf=df_for(keyspace_input,'lineitem') 
pdf=df_for(keyspace_input,'part') 

q_df=sqlContext.sql('''select o.orderkey,p.partkey from orders o JOIN lineitem l on (o.orderkey=l.orderkey) JOIN part p ON (l.partkey=p.partkey)''')


single_RDD=q_df.rdd.map(lambda x:func_topair(x)).reduceByKey(lambda a,b: a+' '+b).coalesce(1)

opRDD=single_RDD.map(lambda (x,y): str(x)+ ' : '+str(y))
opRDD.saveAsTextFile(output)