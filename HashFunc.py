""" All Hash Functions are static to be used in Spark"""


class HashFunc:
    """
    All the hash functions required to calculate PCY algorithms
    """

    @staticmethod
    def pair_bitmap(items):
        """
        Hash function for calculation
        :param items:
        :return:
        """
        mul = 1
        for item in items:
            mul *= item
        return mul % 25

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
