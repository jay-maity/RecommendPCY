from pyspark import SparkContext, SparkConf
import operator


class PCYFrequentItems:
    """
    Find Frequent item list using PCY algorithm
    """

    IS_DEBUGGING = False

    def __init__(self, is_debug):
        """
        Sets the initial debiggin parameter
        :param is_debug: Print collect messages if set true
        """
        self.IS_DEBUGGING = is_debug

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def hash1(items):
        """
        Hash function for calculation
        :param items:
        :return:
        """
        mul = 1
        for item in items:
            mul *= item
        return mul%25

    @staticmethod
    def hash2(items):
        """
        Hash function for calculation
        :param items:
        :return:
        """

        mul = 1
        for item in items:
            mul *= item
        return mul%100000000

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
        order_prod_hash = item_group_rdd\
            .map(lambda x: (hash_function(x), 1))

        # Group, filter and get unique item sets
        frequent_items = order_prod_hash.reduceByKey(operator.add)\
            .filter(lambda x: x[1] > support_count)\
            .map(lambda x: x[0])

        return frequent_items

    def pcy_pass(self, order_prod, pass_no, support_count, hashn, hashnplus1):
        """
        Calculates frequent items and bitmap after n th pass
        :param order_prod:
        :param pass_no: no of pass
        :param support_count:
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
        bitmap_nplus1 = self.pcy_freq_items(order_prod_pairs,
                                            hashnplus1,
                                            support_count)

        return frequent_items_n, bitmap_nplus1

    def frequent_items(self, inputs, output, support_count):
        """Output correlation coefficient without mean formula
            Args:
                inputs:Input file location
                output:Output file location
            """
        # Spark Configuration
        conf = SparkConf().setAppName('FreqItemtest')
        spark_context = SparkContext(conf=conf)

        # File loading
        text = spark_context.textFile(inputs)
        order_prod = text.map(PCYFrequentItems.map_nodes).cache()

        pass_no = 1
        freq_items, bitmap = self.pcy_pass(order_prod,
                                           pass_no,
                                           support_count,
                                           PCYFrequentItems.hash2,
                                           PCYFrequentItems.hash1)
        if self.IS_DEBUGGING:
            print("Frequent "+str(pass_no)+"-group items after pass:"+str(pass_no))
            print(freq_items.collect())

            print("Bitmap for " + str(pass_no+1) + "-group items after pass:" + str(pass_no))
            print(bitmap.collect())


        order_prod.unpersist()

if __name__ == "__main__":
    pcy = PCYFrequentItems(is_debug=True)
    pcy.frequent_items("C:/BigData/SFU/TestData/",None,2)
