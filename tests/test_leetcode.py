import pytest

from pyrio import Stream

from examples import leet_array, leet_bits, leet_dp, leet_hash, leet_math, leet_sort, leet_window


def test_count_vowels_and_consonants():
    string = "123Ab5oc-E6db#bCi9<>"
    vowels = set("AEIOUaeiou")

    assert Stream(string).filter(str.isalpha).partition(lambda ch: ch in vowels).map(
        lambda group: len(tuple(group))
    ).to_list() == [4, 6]


# 125. Valid Palindrome
@pytest.mark.parametrize(
    "string, expected",
    [
        ("A man, a plan, a canal: Panama", True),
        ("race a car", False),
        (" ", True),
        ("a1b2c3c2b1a", True),
        ("abc321", False),
        ("xyyx", True),
        ("aba", True),
        ("z", True),
        ("", True),
        (".,", True),
        ("0P", False),
    ],
)
def test_is_palindrome(string, expected):
    assert leet_array.is_palindrome(string) is expected


# 347. Top K Frequent Elements (order among equal frequencies is unspecified)
@pytest.mark.parametrize(
    "nums, k",
    [
        ([1, 1, 1, 2, 2, 3], 2),
        ([1], 1),
        ([4, 4, 4, 5, 5, 6], 1),
        ([1, 1, 2, 2, 3, 3], 2),  # any 2 of {1,2,3} are valid
    ],
)
def test_top_k_frequent(nums, k):
    result = leet_hash.top_k_frequent(nums, k)
    counts = Stream(nums).group_by(collector=lambda key, group: (key, len(group)))
    assert len(result) == k
    threshold = sorted(counts.values(), reverse=True)[k - 1]
    assert all(counts[key] >= threshold for key in result)


# 387. First Unique Character in a String
@pytest.mark.parametrize(
    "string, expected",
    [
        ("leetcode", 0),
        ("loveleetcode", 2),
        ("aabb", None),
        ("a", 0),
        ("aabbc", 4),
    ],
)
def test_first_unique_character(string, expected):
    assert leet_hash.first_unique_character(string) == expected


# 1480. Running Sum of 1d Array
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 2, 3, 4], [1, 3, 6, 10]),
        ([1, 1, 1, 1, 1], [1, 2, 3, 4, 5]),
        ([3], [3]),
        ([-1, 2, -3, 4], [-1, 1, -2, 2]),
    ],
)
def test_running_sum(nums, expected):
    assert leet_window.running_sum(nums) == expected


# 485. Max Consecutive Ones
# itertools.groupby = consecutive runs; contrast with group_by (full buckets) used elsewhere
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 1, 0, 1, 1, 1], 3),
        ([1, 0, 1, 1, 0, 1], 2),
        ([0, 0, 0], 0),
        ([1, 1, 1], 3),
        ([], 0),
        ([1, 1, 0, 0], 2),
        ([0, 0, 1, 1], 2),
        ([1], 1),
        ([0], 0),
    ],
)
def test_max_consecutive_ones(nums, expected):
    assert leet_window.max_consecutive_ones(nums) == expected


# 349. Intersection of Two Arrays
@pytest.mark.parametrize(
    "nums1, nums2, expected",
    [
        ([1, 2, 2, 1], [2, 2], [2]),
        ([4, 9, 5], [9, 4, 9, 8, 4], [4, 9]),
        ([1, 2, 3], [4, 5, 6], []),
        ([], [1, 2], []),
        ([1, 2], [], []),
    ],
)
def test_intersection_of_two_arrays(nums1, nums2, expected):
    assert leet_hash.intersection_of_two_arrays(nums1, nums2) == set(expected)


# 412. Fizz Buzz
@pytest.mark.parametrize(
    "n, expected",
    [
        (
            15,
            [
                "1",
                "2",
                "Fizz",
                "4",
                "Buzz",
                "Fizz",
                "7",
                "8",
                "Fizz",
                "Buzz",
                "11",
                "Fizz",
                "13",
                "14",
                "FizzBuzz",
            ],
        ),
        (5, ["1", "2", "Fizz", "4", "Buzz"]),
        (1, ["1"]),
    ],
)
def test_fizz_buzz(n, expected):
    assert leet_math.fizz_buzz(n) == expected


# 242. Valid Anagram
@pytest.mark.parametrize(
    "left, right, expected",
    [
        ("anagram", "nagaram", True),
        ("rat", "car", False),
        ("a", "a", True),
        ("ab", "abb", False),
        ("", "a", False),
        ("aaaa", "aaaa", True),
    ],
)
def test_valid_anagram(left, right, expected):
    assert leet_hash.valid_anagram(left, right) is expected


# 136. Single Number
@pytest.mark.parametrize(
    "nums, expected",
    [([2, 2, 1], 1), ([4, 1, 2, 1, 2], 4), ([1], 1), ([-1, -1, -2], -2)],
)
def test_single_number(nums, expected):
    assert leet_bits.single_number(nums) == expected


# 509. Fibonacci Number (sequence form)
@pytest.mark.parametrize(
    "n, expected",
    [(8, [0, 1, 1, 2, 3, 5, 8, 13]), (1, [0]), (2, [0, 1]), (0, [])],
)
def test_fibonacci(n, expected):
    assert leet_math.fibonacci(n) == expected


# 217. Contains Duplicate
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 2, 3, 1], True),
        ([1, 2, 3, 4], False),
        ([1, 1, 1, 3, 3, 4, 3, 2, 4, 2], True),
        ([1], False),
    ],
)
def test_contains_duplicate(nums, expected):
    assert leet_hash.contains_duplicate(nums) is expected


# 771. Jewels and Stones
@pytest.mark.parametrize(
    "jewels, stones, expected",
    [("aA", "aAAbbbb", 3), ("z", "ZZ", 0), ("", "abc", 0), ("aA", "", 0)],
)
def test_jewels_and_stones(jewels, stones, expected):
    assert leet_hash.jewels_and_stones(jewels, stones) == expected


# 905. Sort Array By Parity
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([3, 1, 2, 4], [2, 4, 3, 1]),
        ([0], [0]),
        ([2, 4, 6], [2, 4, 6]),
        ([1, 3, 5], [1, 3, 5]),
    ],
)
def test_sort_array_by_parity(nums, expected):
    assert leet_array.sort_array_by_parity(nums) == expected


# 896. Monotonic Array
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 2, 2, 3], True),
        ([6, 5, 4, 4], True),
        ([1, 3, 2], False),
        ([1], True),
        ([2, 2, 2], True),
        ([], True),
        ([1, 2], True),
        ([2, 1], True),
        ([1, 2, 3, 4], True),
        ([4, 3, 2, 1], True),
    ],
)
def test_is_monotonic(nums, expected):
    assert leet_array.is_monotonic(nums) is expected


# 1876. Substrings of Size Three with Distinct Characters
@pytest.mark.parametrize(
    "string, expected",
    [("xyzzaz", 1), ("aababcabc", 4), ("aaaa", 0), ("ab", 0), ("abc", 1)],
)
def test_substrings_of_size_three_with_distinct_chars(string, expected):
    assert leet_window.substrings_of_size_three_with_distinct_chars(string) == expected


# 1313. Decompress Run-Length Encoded List
@pytest.mark.parametrize(
    "nums, expected",
    [([1, 2, 3, 4], [2, 4, 4, 4]), ([1, 1, 2, 3], [1, 3, 3]), ([5, 1], [1, 1, 1, 1, 1])],
)
def test_decompress_rle_list(nums, expected):
    assert leet_array.decompress_rle_list(nums) == expected


# 414. Third Maximum Number
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([3, 2, 1], 1),
        ([1, 2], 2),
        ([2, 2, 3, 1], 1),
        ([2, 2, 2], 2),
        ([1, 2, 2, 5, 3, 5], 2),
        ([-1, -2, -3, -4], -3),
    ],
)
def test_third_maximum_number(nums, expected):
    assert leet_sort.third_maximum_number(nums) == expected


# 1832. Check if the Sentence Is Pangram
@pytest.mark.parametrize(
    "sentence, expected",
    [
        ("thequickbrownfoxjumpsoverthelazydog", True),
        ("leetcode", False),
        ("abcdefghijklmnopqrstuvwxyz", True),
        ("The Quick Brown Fox Jumps Over The Lazy Dog!", True),
        ("abcdefghijklmnopqrstuvwxy", False),
    ],
)
def test_check_if_pangram(sentence, expected):
    assert leet_hash.check_if_pangram(sentence) is expected


# 1491. Average Salary Excluding the Minimum and Maximum Salary
# LC constraint: salary.length >= 3
@pytest.mark.parametrize(
    "salary, expected",
    [
        ([4000, 3000, 1000, 2000], 2500.0),
        ([1000, 2000, 3000], 2000.0),
        ([1000, 1000, 2000, 3000], 1500.0),
        ([5, 5, 5], 5.0),
    ],
)
def test_average_salary_excluding_minmax(salary, expected):
    assert leet_sort.average_salary_excluding_minmax(salary) == expected


# 1431. Kids With the Greatest Number of Candies
@pytest.mark.parametrize(
    "candies, extra, expected",
    [
        ([2, 3, 5, 1, 3], 3, [True, True, True, False, True]),
        ([4, 2, 1, 1, 2], 1, [True, False, False, False, False]),
        ([12, 1, 12], 10, [True, False, True]),
        ([2, 3, 5, 1, 3], 0, [False, False, True, False, False]),
    ],
)
def test_kids_with_greatest_candies(candies, extra, expected):
    assert leet_math.kids_with_greatest_candies(candies, extra) == expected


# 268. Missing Number
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([3, 0, 1], 2),
        ([0, 1], 2),
        ([1, 2], 0),
        ([0], 1),
        ([1], 0),
        ([9, 6, 4, 2, 3, 5, 7, 0, 1], 8),
    ],
)
def test_missing_number(nums, expected):
    assert leet_bits.missing_number(nums) == expected


# 1684. Count the Number of Consistent Strings
@pytest.mark.parametrize(
    "allowed, words, expected",
    [
        ("ab", ["ad", "bd", "aaab", "baa", "badab"], 2),
        ("abc", ["a", "b", "c", "ab", "ac", "bc", "abc"], 7),
        ("abc", ["", "a", "d"], 2),
        ("", ["", "a"], 1),
        ("a", ["b", "c", "bc"], 0),
        ("a", ["a", "aa", "b"], 2),
    ],
)
def test_count_consistent_strings(allowed, words, expected):
    assert leet_hash.count_consistent_strings(allowed, words) == expected


# 728. Self Dividing Numbers
@pytest.mark.parametrize(
    "left, right, expected",
    [
        (1, 22, [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 22]),
        (47, 85, [48, 55, 66, 77]),
        (10, 12, [11, 12]),  # 10 has a zero digit and is excluded
        (1, 9, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
        (20, 22, [22]),  # 20 has a zero digit and is excluded
    ],
)
def test_self_dividing_numbers(left, right, expected):
    assert leet_math.self_dividing_numbers(left, right) == expected


# 1089. Duplicate Zeros (expanded form; LC is in-place with fixed length)
@pytest.mark.parametrize(
    "arr, expected",
    [
        ([1, 0, 2, 3, 0, 4], [1, 0, 0, 2, 3, 0, 0, 4]),
        ([1, 2, 3], [1, 2, 3]),
        ([0, 1, 0], [0, 0, 1, 0, 0]),
    ],
)
def test_duplicate_zeros(arr, expected):
    assert leet_array.duplicate_zeros(arr) == expected


# 26. Remove Duplicates from Sorted Array
# unique_just_seen only requires adjacent duplicates (sorted input is the LC framing)
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 1, 2], [1, 2]),
        ([0, 0, 1, 1, 1, 2, 2, 3, 3, 4], [0, 1, 2, 3, 4]),
        ([1, 2, 2, 1], [1, 2, 1]),
        ([1, 1, 1], [1]),
        ([1, 2, 3], [1, 2, 3]),
    ],
)
def test_remove_consecutive_duplicates(nums, expected):
    assert leet_array.remove_consecutive_duplicates(nums) == expected


# 70. Climbing Stairs (same recurrence as Fibonacci)
@pytest.mark.parametrize("n, expected", [(1, 1), (2, 2), (3, 3), (5, 8), (10, 89)])
def test_climbing_stairs(n, expected):
    assert leet_dp.climbing_stairs(n) == expected


# 1137. N-th Tribonacci Number
@pytest.mark.parametrize(
    "n, expected",
    [(0, 0), (1, 1), (2, 1), (3, 2), (4, 4), (25, 1389537)],
)
def test_tribonacci(n, expected):
    assert leet_dp.tribonacci(n) == expected


# 198. House Robber (LC: nums.length >= 1)
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 2, 3, 1], 4),
        ([2, 7, 9, 3, 1], 12),
        ([2, 1, 1, 2], 4),
        ([1], 1),
        ([2, 1], 2),
        ([2, 2], 2),
    ],
)
def test_house_robber(nums, expected):
    assert leet_dp.house_robber(nums) == expected


# 746. Min Cost Climbing Stairs
@pytest.mark.parametrize(
    "cost, expected",
    [([10, 15], 10), ([10, 15, 20], 15), ([1, 100, 1, 1, 1, 100, 1, 1, 100, 1], 6)],
)
def test_min_cost_climbing_stairs(cost, expected):
    assert leet_dp.min_cost_climbing_stairs(cost) == expected


# 53. Maximum Subarray
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([-2, 1, -3, 4, -1, 2, 1, -5, 4], 6),
        ([1], 1),
        ([5, 4, -1, 7, 8], 23),
        ([-2, -1], -1),
        ([-1], -1),
    ],
)
def test_maximum_subarray(nums, expected):
    assert leet_dp.maximum_subarray(nums) == expected


# 121. Best Time to Buy and Sell Stock (LC: prices.length >= 1)
@pytest.mark.parametrize(
    "prices, expected",
    [
        ([7, 1, 5, 3, 6, 4], 5),
        ([7, 6, 4, 3, 1], 0),
        ([2, 4, 1], 2),
        ([1], 0),
        ([1, 2], 1),
        ([2, 1], 0),
    ],
)
def test_best_time_to_buy_and_sell_stock(prices, expected):
    assert leet_dp.best_time_to_buy_and_sell_stock(prices) == expected


# 118. Pascal's Triangle
@pytest.mark.parametrize(
    "num_rows, expected",
    [
        (5, [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1], [1, 4, 6, 4, 1]]),
        (1, [[1]]),
        (2, [[1], [1, 1]]),
    ],
)
def test_pascals_triangle(num_rows, expected):
    assert leet_dp.pascals_triangle(num_rows) == expected


# 62. Unique Paths
@pytest.mark.parametrize("m, n, expected", [(3, 7, 28), (3, 2, 3), (1, 1, 1), (1, 3, 1), (3, 1, 1)])
def test_unique_paths(m, n, expected):
    assert leet_dp.unique_paths(m, n) == expected


# 338. Counting Bits
@pytest.mark.parametrize(
    "n, expected",
    [(0, [0]), (2, [0, 1, 1]), (5, [0, 1, 1, 2, 1, 2])],
)
def test_counting_bits(n, expected):
    assert leet_bits.counting_bits(n) == expected
