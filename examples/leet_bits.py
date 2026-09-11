from operator import xor

from pyrio import Stream


# 136. Single Number
def single_number(nums):
    return Stream(nums).reduce(xor).get()


# single_number([4, 1, 2, 1, 2]) => 4


# 268. Missing Number
def missing_number(nums):
    n = len(nums)
    return Stream.from_range(0, n + 1).concat(nums).reduce(xor).get()


# missing_number([3, 0, 1]) => 2


# 338. Counting Bits
def counting_bits(n):
    return Stream.from_range(0, n + 1).map(int.bit_count).to_list()


# counting_bits(5) => [0, 1, 1, 2, 1, 2]
