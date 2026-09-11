from operator import itemgetter

from pyrio import Stream


# 485. Max Consecutive Ones
def max_consecutive_ones(nums):
    return (
        Stream(nums)
        .groupby()
        .filter(itemgetter(0))
        .map(lambda kv: len(tuple(kv[1])))
        .max(default=0)
        .get()
    )


# max_consecutive_ones([1, 1, 0, 1, 1, 1]) => 3


# 1876. Substrings of Size Three with Distinct Characters
def substrings_of_size_three_with_distinct_chars(string):
    return Stream(string).sliding_window(3).quantify(lambda window: len(set(window)) == 3)


# substrings_of_size_three_with_distinct_chars("aababcabc") => 4


# 1480. Running Sum of 1d Array
def running_sum(nums):
    return Stream(nums).accumulate().to_list()


# running_sum([1, 2, 3, 4]) => [1, 3, 6, 10]
