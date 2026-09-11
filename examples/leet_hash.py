from operator import attrgetter, itemgetter

from pyrio import Stream


# 387. First Unique Character in a String
def first_unique_character(string):
    counts = Stream(string).grouped_by(collector=lambda key, group: (key, len(group)))
    return (
        Stream(string)
        .enumerate()
        .find_first(lambda item: counts[item[1]] == 1)
        .map(itemgetter(0))
        .or_else(None)
    )


# first_unique_character("loveleetcode") => 2


# 349. Intersection of Two Arrays
def intersection_of_two_arrays(nums1, nums2):
    seen = set(nums2)
    return Stream(nums1).filter(lambda x: x in seen).distinct().to_set()


# intersection_of_two_arrays([1, 2, 2, 1], [2, 2]) => {2}


# 242. Valid Anagram
def valid_anagram(left, right):
    return Stream(left).sort().compare_with(Stream(right).sort())


# valid_anagram("anagram", "nagaram") => True


# 217. Contains Duplicate
def contains_duplicate(nums):
    return len(nums) != Stream(nums).distinct().len()


# contains_duplicate([1, 2, 3, 1]) => True


# 771. Jewels and Stones
def jewels_and_stones(jewels, stones):
    jewel_set = set(jewels)
    return Stream(stones).quantify(lambda ch: ch in jewel_set)


# jewels_and_stones("aA", "aAAbbbb") => 3


# 1832. Check if the Sentence Is Pangram
def check_if_pangram(sentence):
    return Stream(sentence).filter(str.isalpha).map(str.lower).distinct().len() == 26


# check_if_pangram("thequickbrownfoxjumpsoverthelazydog") => True


# 1684. Count the Number of Consistent Strings
def count_consistent_strings(allowed, words):
    allowed = set(allowed)
    return Stream(words).filter(lambda word: set(word) <= allowed).len()


# count_consistent_strings("ab", ["ad", "bd", "aaab", "baa", "badab"]) => 2


# 347. Top K Frequent Elements
def top_k_frequent(nums, k):
    counts = Stream(nums).grouped_by(collector=lambda key, group: (key, len(group)))
    return (
        Stream(counts)
        .sort(attrgetter("value"), reverse=True)
        .limit(k)
        .map(attrgetter("key"))
        .to_set()
    )


# top_k_frequent([1, 1, 1, 2, 2, 3], 2) => {1, 2}
