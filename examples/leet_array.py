from pyrio import Stream


# 125. Valid Palindrome
def is_palindrome(string: str) -> bool:
    normalized = Stream(string).filter(str.isalnum).map(str.lower).to_string("")
    return Stream(normalized).compare_with(reversed(normalized))


# is_palindrome("A man, a plan, a canal: Panama") => True


# 905. Sort Array By Parity
def sort_array_by_parity(nums: list[int]) -> list[int]:
    return Stream(nums).partition(lambda x: x % 2 == 0).flatten().to_list()


# sort_array_by_parity([3, 1, 2, 4]) => [2, 4, 3, 1]


# 26. Remove Duplicates from Sorted Array
def remove_consecutive_duplicates(nums: list[int]) -> list[int]:
    return Stream(nums).unique_just_seen().to_list()


# remove_consecutive_duplicates([0, 0, 1, 1, 1, 2, 2, 3, 3, 4]) => [0, 1, 2, 3, 4]


# 1089. Duplicate Zeros
def duplicate_zeros(arr: list[int]) -> list[int]:
    return Stream(arr).flat_map(lambda v: [v] * (2 if v == 0 else 1)).to_list()


# duplicate_zeros([1, 0, 2, 3, 0, 4]) => [1, 0, 0, 2, 3, 0, 0, 4]


# 1313. Decompress Run-Length Encoded List
def decompress_rle_list(nums: list[int]) -> list[int]:
    return Stream(nums).grouper(2).flat_map(lambda pair: [pair[1]] * pair[0]).to_list()


# decompress_rle_list([1, 2, 3, 4]) => [2, 4, 4, 4]


# 896. Monotonic Array
def is_monotonic(nums: list[int]) -> bool:
    increasing = Stream(nums).pairwise().all_match(lambda pair: pair[0] <= pair[1])
    decreasing = Stream(nums).pairwise().all_match(lambda pair: pair[0] >= pair[1])
    return increasing or decreasing


# is_monotonic([1, 2, 2, 3]) => True
