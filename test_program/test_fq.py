from cassandra.cluster import Cluster
import math
import random
from random import sample

cluster = Cluster(['199.60.17.136', '199.60.17.173'])
session = cluster.connect('anajeeb')

#session.execute('CREATE TABLE IF NOT EXISTS bitmap (bucket bigint,PRIMARY KEY (bucket));')

#removed = random.sample(range(1,1000),699)
#for i in removed:
#    session.execute('delete from bitmap where bucket = %d;' %i)
 

pair_a = random.sample(range(1,200000),199999)
pair_b = random.sample(range(1,200000),199999)
count = 0


"""
for i,j in zip(pair_a,pair_b):
    
    hash_num = ((i*j)%1000)
    print 'going for cassandra \n'
    test = session.execute('select bucket from bitmap where bucket = %d' %hash_num)
    print 'out of cassandra \n'
    if (test):
        print 'hashed!! pair (%d,%d)'%(i,j)
    else:
        print 'not hashed!! '    

"""


for i,j in zip(pair_a,pair_b):

    hash_num = ((i*j)%1000)
    print '\n------SEARCH: pair (%d,%d)------\n'%(i,j)
    print 'going for cassandra \n'
    test1 = session.execute('select item from fq_items where item = %d' %i)
    test2 = session.execute('select item from fq_items where item = %d' %j)
    test3 = session.execute('select bucket from bitmap where bucket = %d' %hash_num)
    print 'out of cassandra \n'
    
    if (test1 and test2 and test3):
        print '\n\n WAIT!!!!!!!! \n\n'
        print 'pair item (%d,%d) found in FREQUENT ITEM LIST & in FREQUENT BUCKET LIST\n'%(i,j)
        count = count + 1
    else:
        print 'One of the items in (%d,%d) NOT FOUND OR not hashed to bitmap\n'%(i,j) 
        continue
        
    print '\n------SEARCH for pair (%d,%d)COMPLETE------\n'%(i,j)   
    
    
print '\n------ALL SEARCH COMPLETE------\n'
print '\n\n\n'
print 'total %d pairs found frequent'%count
