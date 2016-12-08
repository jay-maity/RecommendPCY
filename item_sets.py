""" Frequent item discovery by PCY algorithm"""

import operator
import json
import sys
from pyspark import SparkContext, SparkConf
import pyspark_cassandra
from cassandra.cluster import Cluster


cluster = None
session = None


class PCYFrequentItems:
    """
    Find Frequent item list using PCY algorithm
    """

    IS_DEBUGGING = False
    config_object = None

    def __init__(self, is_debug, config_file="config.json"):
        """
        Sets the initial debiggin parameter
        :param is_debug: Print collect messages if set true
        """
        self.IS_DEBUGGING = is_debug
        json_data = open(config_file).read()
        self.config_object = json.loads(json_data)

    @staticmethod
    def group_items(basket, group_size):
        """
        Get item_groups from a basket
        Returns sorted items by their numerical number
        :param basket: Basket to search the item_group from (could be a single cart)
        :param group_size: Size of the item_group to form
        :return:
        """
        assert (group_size >= 1 and isinstance(group_size, int)), \
            "Please use group size as Integer and  > 0"

        # In case of group size is one simply return each items
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
                base_item_count = i + (group_size - 1)
                base_items = basket[i:base_item_count]

                for item in basket[base_item_count:]:
                    item_groups.append(tuple(base_items) + (item,))
        return item_groups

    @staticmethod
    def map_nodes(line):
        """
        Map line into graph node key = value as array
        """
        key_values = line.split(":")
        # key = int(key_values[0])
        values = []
        if key_values[1].strip() != "":
            values = [int(node) for node in key_values[1].strip().split(' ')]
        return values

    @staticmethod
    def filter_pairs(pair, hosts, keyspace, hashfunction, item_table, bitmap_table):
        """
        Filter pairs by querying from cassandra table
        :return:
        """

        global cluster, session
        if cluster is None:
            cluster = Cluster(hosts)
            session = cluster.connect(keyspace)

        item1 = session.execute("select item from "
                                + item_table
                                + " where item = %d" % pair[0])

        item2 = session.execute("select item from "
                                + item_table
                                + " where item = %d" % pair[1])

        bitmap = session.execute("select hash from "
                                 + bitmap_table
                                 + " where hash = %d" % hashfunction(pair))

        print("Pair checked " + str(pair[0]))

        return item1 and item2 and bitmap

    @staticmethod
    def filter_pairs_broadcast(pair, freq_pair, bitmap, hashfunction):
        """
        Filter pairs from broadcast variables
        :return:
        """

        return pair[0] in freq_pair and pair[1] in freq_pair and hashfunction(pair) in bitmap

    def pcy_freq_items(self, item_group_rdd, hash_function, support_count):
        """
        Get Frequent items for a particular group of items
        :param item_group_rdd:
        :param passno:
        :param hash_function:
        :param support_count:
        :return:
        """
        # Hash and Items mapping
        order_prod_hash = item_group_rdd \
            .map(lambda x: (hash_function(x), 1))

        # Group, filter and get unique item sets
        frequent_items = order_prod_hash.reduceByKey(operator.add) \
            .filter(lambda x: x[1] > support_count) \
            .map(lambda x: x[0])

        return frequent_items

    def pcy_pass(self, order_prod, pass_no, support_count, hashn, hashnplus1,
                 is_nplus1_cache=False):
        """
        Calculates frequent items and bitmap after n th pass
        :param order_prod:
        :param pass_no:
        :param support_count:
        :param hashn:
        :param hashnplus1:
        :param is_nplus1_cache:
        :return:
        """
        item_set_count = pass_no
        order_prod_single = order_prod. \
            flatMap(lambda x: PCYFrequentItems.
                    group_items(x, item_set_count))

        frequent_items_n = self.pcy_freq_items(order_prod_single,
                                               hashn,
                                               support_count)
        item_set_count += 1

        order_prod_pairs = order_prod. \
            flatMap(lambda x: PCYFrequentItems.group_items(x, item_set_count))

        if is_nplus1_cache:
            order_prod_pairs = order_prod_pairs.cache()

        bitmap_nplus1 = self.pcy_freq_items(order_prod_pairs,
                                            hashnplus1,
                                            support_count)

        return frequent_items_n, bitmap_nplus1, order_prod_pairs

    @staticmethod
    def pair_bitmap(items):
        """
        Hash function for calculation for pairs
        :param items:
        :return:
        """

        mul = 1
        for item in items:
            mul *= ((2 * item) + 1)
        return mul % 999917

    @staticmethod
    def single(items):
        """
        Hash function for calculation
        :param items:
        :return:
        """

        mul = 1
        for item in items:
            mul *= item
        return mul % 100000000

    def configure(self):
        """
        Configure spark and cassandra objects
        :param is_local_host:
        :return:
        """
        # Spark Configuration
        conf = SparkConf().setAppName('Frequent Item Sets'). \
            set('spark.cassandra.connection.host', ','.join(self.config_object["CassandraHosts"]))
        return SparkContext(conf=conf)

    def frequent_items(self, inputs, output, support_count, is_broadcast=True):
        """Output correlation coefficient without mean formula
            Args:
                inputs:Input file location
                output:Output file location
                support_count:
                is_broadcast: Item pair will be found using broadcast or not
            """

        spark_context = self.configure()

        # File loading
        text = spark_context.textFile(inputs)
        order_prod = text.map(PCYFrequentItems.map_nodes).cache()

        pass_no = 1
        freq_items, bitmap, all_pairs = self.pcy_pass(order_prod,
                                                      pass_no,
                                                      support_count,
                                                      PCYFrequentItems.single,
                                                      PCYFrequentItems.pair_bitmap,
                                                      is_nplus1_cache=True)
        if self.IS_DEBUGGING:
            print("Frequent " + str(pass_no) + "-group items after pass:" + str(pass_no))
            print(freq_items.collect())

            print("Bitmap for " + str(pass_no + 1) + "-group items after pass:" + str(pass_no))
            print(bitmap.collect())

        # System will use broadcast based on user input
        if is_broadcast:

            bitmap_set = set(bitmap.collect())
            freq_items_set = set(freq_items.collect())

            bitmap_broadast = spark_context.broadcast(bitmap_set)
            freq_items_set = spark_context.broadcast(freq_items_set)

            frequent_pairs = all_pairs.filter(lambda x:
                                              PCYFrequentItems.
                                              filter_pairs_broadcast(x,
                                                                     freq_items_set.value,
                                                                     bitmap_broadast.value,
                                                                     PCYFrequentItems.pair_bitmap
                                                                     ))

        else:
            # Making freq items Ready to save to cassandra
            freq_items = freq_items.map(lambda x: {'item': x})
            freq_items.saveToCassandra(self.config_object["KeySpace"],
                                       self.config_object["Item1Table"])

            # Making bitmap Ready to save to cassandra
            bitmap = bitmap.map(lambda x: {'hash': x})
            bitmap.saveToCassandra(self.config_object["KeySpace"],
                                   self.config_object["Bitmap2Table"])

            print(all_pairs.count())

            frequent_pairs = all_pairs.filter(lambda x: PCYFrequentItems.
                                              filter_pairs(x,
                                                           self.config_object["CassandraHosts"],
                                                           self.config_object["KeySpace"],
                                                           PCYFrequentItems.pair_bitmap,
                                                           self.config_object["Item1Table"],
                                                           self.config_object["Bitmap2Table"]))

        if self.IS_DEBUGGING:
            print(all_pairs.collect())
            print(frequent_pairs.collect())

        # Saves as text file
        frequent_pairs.saveAsTextFile(output)

        frequent_pairs = frequent_pairs.\
            map(lambda x: {'productid1': x[0], 'productid2': x[1]})
        # Save final output to cassandra
        frequent_pairs.saveToCassandra(self.config_object["KeySpace"],
                                       self.config_object["RecommendTable"])

        all_pairs.unpersist()
        order_prod.unpersist()


def main():
    """
    Handles parameters for the file to run
    :return:
    """
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    support_thresold = int(sys.argv[3])

    broadcast = 1
    if len(sys.argv) > 4:
        broadcast = int(sys.argv[4])

    pcy = PCYFrequentItems(is_debug=True)
    if broadcast == 1:
        is_broadcast = True
    else:
        is_broadcast = False

    pcy.frequent_items(input_path, output_path, support_thresold, is_broadcast)


if __name__ == "__main__":
    main()
