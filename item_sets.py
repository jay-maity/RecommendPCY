import operator
import sys
import re
import math
from pyspark import SparkContext, SparkConf

IS_DEBUGGING = True

def group_items(basket, group_size):
    """
    Get item_groups from a basket
    Returns sorted items by their numerical number
    :param basket: Basket to search the item_group from (could be a single cart)
    :param group_size: Size of the item_group to form
    :return:
    """
    #assert (group_size > 1) , "Use group size > 1 otherwise consider each element"
    if group_size == 1:
        return [(item,) for item in basket]
    item_groups = []
    if len(basket) >= group_size:
        # Sort the basket
        basket = sorted(basket)
        # Loop through the basket
        for i in range(len(basket) - group_size + 1):
            # Gets the base and add all items for each group
            # until end
            # If base is [2,3] and basket is [2,3,4,5]
            # then creates [2,3,4], [2,3,5]
            base_item_count = i + (group_size -1)
            base_items = basket[i:base_item_count]

            for item in basket[base_item_count:]:
                item_groups.append(tuple(base_items) + (item,))
    return item_groups

def map_nodes(line):
    """
    Map line into graph node key = value as array
    """
    key_values = line.split(":")
    key = int(key_values[0])
    values = []
    if key_values[1].strip() != "":
        values = [int(node) for node in key_values[1].strip().split(' ')]
    return values

def combine(tup):
    newtup = []
    for t in tup:
        newtup.extend(t)
    return newtup

def hash1(items):
    mul = 1
    for item in items:
        mul *= item
    return mul%25

def hash2(items):
    mul = 1
    for item in items:
        mul *= item
    return mul%7

def unique_set(item_group_hash):
    val = item_group_hash[1]
    key = item_group_hash[0]

    used = set()
    for item in val:
        if item not in used:
            used.add(item)
    return (key, list(used))


def pcy_freq_items(item_group_rdd, passno, hash_function, support_count):
    """
    Get Candidate keys for next pass
    :param item_group_rdd:
    :param passno:
    :param hash_function:
    :param support_count:
    :return:
    """
    # Hash and Items mapping
    order_prod_hash = item_group_rdd\
        .map(lambda x: ((hash_function(x), x)))

    # Group, filter and get unique item sets
    frequent_items = order_prod_hash.groupByKey()\
        .filter(lambda x: len(x[1]) > support_count)\
        .map(unique_set)

    if IS_DEBUGGING:
        print("Frequent item sets with hash after pass:" + str(passno))
        print(frequent_items.collect())

    # Get only frequent items by ignoring hash values
    frequent_items = frequent_items.map(lambda x: x[1])
    return frequent_items


#def pcy_candidate(item_group_rdd, passno, hash_function, support_count):


def test1(inputs, output):
    """Output correlation coefficient without mean formula
        Args:
            inputs:Input file location
            output:Output file location
        """
    conf = SparkConf().setAppName('FreqItemtest')
    spark_context = SparkContext(conf=conf)
    text = spark_context.textFile(inputs)
    order_prod = text.map(map_nodes)

    passno = 1
    order_prod_pairs = order_prod.flatMap(lambda x: group_items(x, passno)).cache()

    rdd_pass1 = pcy_freq_items(order_prod_pairs, 1, hash1, 2)

    if IS_DEBUGGING:
        print("After pass1:")
        print(rdd_pass1.collect())

    order_prod_pairs.unpersist()


    # #Pass2
    # # Convert list of lists to list by using flatMap
    # order_prod_pairs = order_prod.flatMap(lambda x: group_items(x, 2)).cache()
    # order_prod_pairs_hash = order_prod_pairs.map(lambda x: ((hash1(x), x)))
    #
    # # Skipping this step to avoid Join
    # #order_prod_hash = order_prod_pairs.map(lambda x: ((hash1(x),1)))
    #
    # pair_count = order_prod_pairs_hash.groupByKey().filter(lambda x: len(x[1]) > 3)
    # new_pairs = pair_count.flatMap(lambda x: x[1])
    #
    # print(new_pairs.collect())

test1("C:/BigData/SFU/TestData/",None)