from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext, SparkConf
import sys
#import scala.collection.mutable.ArrayBuffer 
#import scala.collection.mutable.ListBuffer

input = sys.argv[1]
output = sys.argv[2]


conf = SparkConf().setAppName('fpgrowth')
sc = SparkContext(conf=conf)
data = sc.textFile(input)
transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.3, numPartitions=10)
result = model.freqItemsets()
"""
result = model.freqItemsets()
model = FPGrowth.train(transactions, minSupport=0.3, numPartitions=10)
result = model.freqItemsets().collect()

for fi in result:
    print(fi)
"""
