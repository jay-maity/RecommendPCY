from pyspark import SparkContext, SparkConf


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
        return mul%1000000

    @staticmethod
    def unique_set(item_group_hash):
        """
        Get unique set from repeative items
        :param item_group_hash:
        :return:
        """
        val = item_group_hash[1]
        key = item_group_hash[0]

        used = set()
        for item in val:
            if item not in used:
                used.add(item)
        return (key, list(used))

    def pcy_freq_items(self, item_group_rdd, passno, hash_function, support_count):
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
            .filter(lambda x: len(x[1]) > support_count)

        # Takes only unique items from repeative item sets
        frequent_items_unique = frequent_items \
            .map(PCYFrequentItems.unique_set)

        if self.IS_DEBUGGING:
            frequent_items_count = frequent_items \
                .map(lambda x: (x[0], len(x[1])))
            print("Frequent item sets with hash after pass:" + str(passno))
            print(frequent_items_count.collect())

        # Get only frequent items by ignoring hash values
        if passno == 1:
            # Get rid of tuple if it is a single value
            frequent_items_unique = frequent_items_unique.flatMap(lambda x: x[1][0])
        else:
            frequent_items_unique = frequent_items_unique.flatMap(lambda x: x[1])

        return frequent_items_unique

    @staticmethod
    def remove_infrequent_items(frequent_item_set, items):
        """
        Remove infrequent items from item list
        :param frequent_item_set:
        :param items:
        :return:
        """
        new_items = []
        for item in items:
            if item in frequent_item_set:
                new_items.append(item)

        return new_items

    @staticmethod
    def rm_comp_prev_item_set(frequent_item_set, items):
        """
        Remove infrequent items from item list
        :param frequent_item_set:
        :param items:
        :return:
        """
        for item in items:
            if item not in frequent_item_set:
                return None

        return items

    def frequent_items(self, inputs, output):
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

        if self.IS_DEBUGGING:
            print("Initial item list")
            print(order_prod.collect())

        # ####################### Pass 1 ############################
        item_set_count = 1
        order_prod_single = order_prod.\
            flatMap(lambda x: PCYFrequentItems.
                    group_items(x, item_set_count))

        rdd_pass1 = self.pcy_freq_items(order_prod_single,
                                        item_set_count,
                                        PCYFrequentItems.
                                        hash2,
                                        2)

        # order_prod = order_prod\
        #     .map(lambda items: PCYFrequentItems.remove_infrequent_items(bc_pass1.value, items))

        if self.IS_DEBUGGING:
            print("After Item set count1:")
            print(rdd_pass1.collect())

            print("New list after removing infrequent items")
            print(order_prod.collect())

        # #########################################################################################

        item_set_count = 2

        # Broadcast way
        # Estimation: for 1 million items it can take up to 30mb to create a set,
        # which seems reasonable to broadcast
        # TODO: Find a way to split broadcast variable and run in a loop
        bc_item_set1 = spark_context.broadcast(set(rdd_pass1.collect()))

        order_prod_pairs = order_prod.\
            flatMap(lambda x: PCYFrequentItems.group_items(x, item_set_count))

        rdd_pass2 = self.pcy_freq_items(order_prod_pairs, 2, PCYFrequentItems.hash1, 2)
        freq_item_set2 = rdd_pass2.\
            map(lambda x: PCYFrequentItems.rm_comp_prev_item_set(bc_item_set1.value, x))

        if self.IS_DEBUGGING:
            print("Print broadcast value")
            print(bc_item_set1.value)

            print("After item set count 2 rdd collect:")
            print(rdd_pass2.collect())

            print("After item set count 2 freq item set collect:")
            print(freq_item_set2.collect())

        bc_item_set1.destroy()
        order_prod.unpersist()



if __name__ == "__main__":
    pcy = PCYFrequentItems(is_debug=False)
    pcy.frequent_items("C:/BigData/SFU/TestData/",None)
