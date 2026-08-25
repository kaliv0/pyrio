from operator import attrgetter, itemgetter, xor

import pytest

from pyrio import Stream


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
    ],
)
def test_is_palindrome(string, expected):
    normalized = Stream(string).filter(str.isalnum).map(str.lower).to_string("")
    assert Stream(normalized).compare_with(reversed(normalized)) is expected


# 347. Top K Frequent Elements (order among equal frequencies is unspecified)
@pytest.mark.parametrize(
    "nums, k, expected",
    [
        ([1, 1, 1, 2, 2, 3], 2, [1, 2]),
        ([1], 1, [1]),
        ([4, 4, 4, 5, 5, 6], 1, [4]),
        ([1, 1, 2, 2, 3, 3], 2, [1, 2]),
    ],
)
def test_top_k_frequent(nums, k, expected):
    counts = Stream(nums).group_by(collector=lambda key, group: (key, len(group)))
    assert Stream(counts).sort(attrgetter("value"), reverse=True).limit(k).map(
        attrgetter("key")
    ).to_set() == set(expected)


# 387. First Unique Character in a String
@pytest.mark.parametrize(
    "string, expected",
    [("leetcode", 0), ("loveleetcode", 2), ("aabb", None)],
)
def test_first_unique_character(string, expected):
    counts = Stream(string).group_by(collector=lambda key, group: (key, len(group)))
    assert (
        Stream(string)
        .enumerate()
        .find_first(lambda item: counts[item[1]] == 1)
        .map(itemgetter(0))
        .or_else(None)
        == expected
    )


# 1480. Running Sum of 1d Array
@pytest.mark.parametrize(
    "nums, expected",
    [([1, 2, 3, 4], [1, 3, 6, 10]), ([1, 1, 1, 1, 1], [1, 2, 3, 4, 5]), ([3], [3])],
)
def test_running_sum(nums, expected):
    assert Stream(nums).accumulate().to_list() == expected


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
    ],
)
def test_max_consecutive_ones(nums, expected):
    assert (
        Stream(nums)
        .groupby()
        .filter(itemgetter(0))  # lambda kv: kv[0] == 1
        .map(lambda kv: len(tuple(kv[1])))
        .max(default=0)
        .get()
        == expected
    )


# 349. Intersection of Two Arrays
@pytest.mark.parametrize(
    "nums1, nums2, expected",
    [
        ([1, 2, 2, 1], [2, 2], [2]),
        ([4, 9, 5], [9, 4, 9, 8, 4], [4, 9]),
        ([1, 2, 3], [4, 5, 6], []),
    ],
)
def test_intersection_of_two_arrays(nums1, nums2, expected):
    seen = set(nums2)
    assert Stream(nums1).filter(lambda x: x in seen).distinct().to_set() == set(expected)


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
    def label(num):
        match (num % 3 == 0, num % 5 == 0):
            case (True, True):
                return "FizzBuzz"
            case (True, False):
                return "Fizz"
            case (False, True):
                return "Buzz"
            case _:
                return str(num)

    assert Stream.from_range(1, n + 1).map(label).to_list() == expected


# 242. Valid Anagram
@pytest.mark.parametrize(
    "left, right, expected",
    [
        ("anagram", "nagaram", True),
        ("rat", "car", False),
        ("a", "a", True),
        ("ab", "abb", False),
        ("", "a", False),
    ],
)
def test_valid_anagram(left, right, expected):
    assert Stream(left).sort().compare_with(Stream(right).sort()) is expected


# 136. Single Number
@pytest.mark.parametrize(
    "nums, expected",
    [([2, 2, 1], 1), ([4, 1, 2, 1, 2], 4), ([1], 1)],
)
def test_single_number(nums, expected):
    assert Stream(nums).reduce(xor).get() == expected


# 509. Fibonacci Number (sequence form)
@pytest.mark.parametrize(
    "n, expected",
    [(8, [0, 1, 1, 2, 3, 5, 8, 13]), (1, [0]), (2, [0, 1]), (0, [])],
)
def test_fibonacci(n, expected):
    assert (
        Stream.iterate((0, 1), lambda pair: (pair[1], pair[0] + pair[1]))
        .map(lambda pair: pair[0])
        .limit(n)
        .to_list()
        == expected
    )


# 217. Contains Duplicate
@pytest.mark.parametrize(
    "nums, expected",
    [([1, 2, 3, 1], True), ([1, 2, 3, 4], False), ([1, 1, 1, 3, 3, 4, 3, 2, 4, 2], True)],
)
def test_contains_duplicate(nums, expected):
    assert (len(nums) != Stream(nums).distinct().len()) is expected


# 771. Jewels and Stones
@pytest.mark.parametrize(
    "jewels, stones, expected",
    [("aA", "aAAbbbb", 3), ("z", "ZZ", 0), ("", "abc", 0), ("aA", "", 0)],
)
def test_jewels_and_stones(jewels, stones, expected):
    jewel_set = set(jewels)
    assert Stream(stones).quantify(lambda ch: ch in jewel_set) == expected


# 905. Sort Array By Parity
@pytest.mark.parametrize(
    "nums, expected",
    [([3, 1, 2, 4], [2, 4, 3, 1]), ([0], [0]), ([2, 4, 6], [2, 4, 6])],
)
def test_sort_array_by_parity(nums, expected):
    assert Stream(nums).partition(lambda x: x % 2 == 0).flatten().to_list() == expected


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
    ],
)
def test_is_monotonic(nums, expected):
    increasing = Stream(nums).pairwise().all_match(lambda pair: pair[0] <= pair[1])
    decreasing = Stream(nums).pairwise().all_match(lambda pair: pair[0] >= pair[1])
    assert (increasing or decreasing) is expected


# 1876. Substrings of Size Three with Distinct Characters
@pytest.mark.parametrize(
    "string, expected",
    [("xyzzaz", 1), ("aababcabc", 4), ("aaaa", 0)],
)
def test_substrings_of_size_three_with_distinct_chars(string, expected):
    assert (
        Stream(string).sliding_window(3).quantify(lambda window: len(set(window)) == 3) == expected
    )


# 1313. Decompress Run-Length Encoded List
@pytest.mark.parametrize(
    "nums, expected",
    [([1, 2, 3, 4], [2, 4, 4, 4]), ([1, 1, 2, 3], [1, 3, 3])],
)
def test_decompress_rle_list(nums, expected):
    assert Stream(nums).grouper(2).flat_map(lambda pair: [pair[1]] * pair[0]).to_list() == expected


# 414. Third Maximum Number
@pytest.mark.parametrize(
    "nums, expected",
    [([3, 2, 1], 1), ([1, 2], 2), ([2, 2, 3, 1], 1)],
)
def test_third_maximum_number(nums, expected):
    ranked = Stream(nums).distinct().reverse().to_list()
    assert Stream(ranked).take_nth(2, default=ranked[0]).get() == expected

    # alternative - no intermediate list
    assert Stream(nums).distinct().reverse().take_nth(2, default=max(nums)).get() == expected


# 1832. Check if the Sentence Is Pangram
@pytest.mark.parametrize(
    "sentence, expected",
    [
        ("thequickbrownfoxjumpsoverthelazydog", True),
        ("leetcode", False),
        ("abcdefghijklmnopqrstuvwxyz", True),
        ("The Quick Brown Fox Jumps Over The Lazy Dog!", True),
    ],
)
def test_check_if_pangram(sentence, expected):
    assert (Stream(sentence).filter(str.isalpha).map(str.lower).distinct().len() == 26) is expected


# 1491. Average Salary Excluding the Minimum and Maximum Salary
# LC constraint: salary.length >= 3
@pytest.mark.parametrize(
    "salary, expected",
    [([4000, 3000, 1000, 2000], 2500.0), ([1000, 2000, 3000], 2000.0)],
)
def test_average_salary_excluding_minmax(salary, expected):
    assert Stream(salary).sort().skip(1).limit(len(salary) - 2).average() == expected


# 1431. Kids With the Greatest Number of Candies
@pytest.mark.parametrize(
    "candies, extra, expected",
    [
        ([2, 3, 5, 1, 3], 3, [True, True, True, False, True]),
        ([4, 2, 1, 1, 2], 1, [True, False, False, False, False]),
    ],
)
def test_kids_with_greatest_candies(candies, extra, expected):
    greatest = max(candies)
    assert Stream(candies).map(lambda count: count + extra >= greatest).to_list() == expected


# 268. Missing Number
@pytest.mark.parametrize(
    "nums, expected",
    [([3, 0, 1], 2), ([0, 1], 2), ([9, 6, 4, 2, 3, 5, 7, 0, 1], 8)],
)
def test_missing_number(nums, expected):
    n = len(nums)
    assert Stream.from_range(0, n + 1).concat(nums).reduce(xor).get() == expected


# 1684. Count the Number of Consistent Strings
@pytest.mark.parametrize(
    "allowed, words, expected",
    [
        ("ab", ["ad", "bd", "aaab", "baa", "badab"], 2),
        ("abc", ["a", "b", "c", "ab", "ac", "bc", "abc"], 7),
        ("abc", ["", "a", "d"], 2),
        ("", ["", "a"], 1),
    ],
)
def test_count_consistent_strings(allowed, words, expected):
    allowed = set(allowed)
    assert Stream(words).filter(lambda word: set(word) <= allowed).len() == expected


# 728. Self Dividing Numbers
@pytest.mark.parametrize(
    "left, right, expected",
    [
        (1, 22, [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 22]),
        (47, 85, [48, 55, 66, 77]),
        (10, 12, [11, 12]),  # 10 has a zero digit and is excluded
    ],
)
def test_self_dividing_numbers(left, right, expected):
    def is_self_dividing(num):
        # make digits iterable: 22 -> "22" -> map(int) -> 2, 2
        return Stream(str(num)).map(int).all_match(lambda digit: digit != 0 and num % digit == 0)

    assert Stream.from_range(left, right + 1).filter(is_self_dividing).to_list() == expected


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
    assert Stream(arr).flat_map(lambda v: [v] * (2 if v == 0 else 1)).to_list() == expected


# 26. Remove Duplicates from Sorted Array
# unique_just_seen only requires adjacent duplicates (sorted input is the LC framing)
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([1, 1, 2], [1, 2]),
        ([0, 0, 1, 1, 1, 2, 2, 3, 3, 4], [0, 1, 2, 3, 4]),
        ([1, 2, 2, 1], [1, 2, 1]),
    ],
)
def test_remove_consecutive_duplicates(nums, expected):
    assert Stream(nums).unique_just_seen().to_list() == expected


# 70. Climbing Stairs (same recurrence as Fibonacci)
@pytest.mark.parametrize("n, expected", [(1, 1), (2, 2), (3, 3), (5, 8)])
def test_climbing_stairs(n, expected):
    assert (
        Stream.iterate((1, 1), lambda pair: (pair[1], pair[0] + pair[1]))
        .map(lambda pair: pair[0])
        .take_nth(n)
        .get()
        == expected
    )


# 1137. N-th Tribonacci Number
@pytest.mark.parametrize("n, expected", [(4, 4), (25, 1389537), (0, 0), (1, 1)])
def test_tribonacci(n, expected):
    assert (
        Stream.iterate((0, 1, 1), lambda triple: (triple[1], triple[2], sum(triple)))
        .map(lambda triple: triple[0])
        .take_nth(n)
        .get()
        == expected
    )


# 198. House Robber (LC: nums.length >= 1)
@pytest.mark.parametrize(
    "nums, expected",
    [([1, 2, 3, 1], 4), ([2, 7, 9, 3, 1], 12), ([2, 1, 1, 2], 4), ([1], 1)],
)
def test_house_robber(nums, expected):
    def step(state, value):
        prev, curr = state
        return curr, max(curr, prev + value)

    assert Stream(nums).reduce(step, identity=(0, 0)).map(lambda state: state[1]).get() == expected


# 746. Min Cost Climbing Stairs
@pytest.mark.parametrize(
    "cost, expected",
    [([10, 15, 20], 15), ([1, 100, 1, 1, 1, 100, 1, 1, 100, 1], 6)],
)
def test_min_cost_climbing_stairs(cost, expected):
    def step(state, value):
        return state[1], value + min(state[0], state[1])

    assert Stream(cost).reduce(step, identity=(0, 0)).map(min).get() == expected


# 53. Maximum Subarray
@pytest.mark.parametrize(
    "nums, expected",
    [
        ([-2, 1, -3, 4, -1, 2, 1, -5, 4], 6),
        ([1], 1),
        ([5, 4, -1, 7, 8], 23),
        ([-2, -1], -1),
    ],
)
def test_maximum_subarray(nums, expected):
    def step(state, value):
        best_ending, best_so_far = state
        best_ending = max(value, best_ending + value)
        return best_ending, max(best_so_far, best_ending)

    assert (
        Stream(nums[1:]).reduce(step, identity=(nums[0], nums[0])).map(lambda state: state[1]).get()
        == expected
    )


# 121. Best Time to Buy and Sell Stock (LC: prices.length >= 1)
@pytest.mark.parametrize(
    "prices, expected",
    [([7, 1, 5, 3, 6, 4], 5), ([7, 6, 4, 3, 1], 0), ([2, 4, 1], 2), ([1], 0)],
)
def test_best_time_to_buy_and_sell_stock(prices, expected):
    def step(state, price):
        lowest, best = state
        return min(lowest, price), max(best, price - lowest)

    assert (
        Stream(prices).reduce(step, identity=(prices[0], 0)).map(lambda state: state[1]).get()
        == expected
    )


# 118. Pascal's Triangle
@pytest.mark.parametrize(
    "num_rows, expected",
    [
        (5, [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1], [1, 4, 6, 4, 1]]),
        (1, [[1]]),
    ],
)
def test_pascals_triangle(num_rows, expected):
    def next_row(row):
        middles = Stream(row).pairwise().map(lambda pair: pair[0] + pair[1]).to_list()
        return [1, *middles, 1]

    assert Stream.iterate([1], next_row).limit(num_rows).to_list() == expected


# 62. Unique Paths
@pytest.mark.parametrize("m, n, expected", [(3, 7, 28), (3, 2, 3), (1, 1, 1)])
def test_unique_paths(m, n, expected):
    assert (
        Stream.iterate([1] * n, lambda row: Stream(row).accumulate().to_list())
        .take_nth(m - 1)
        .map(lambda x: x[-1])
        .get()
        == expected
    )


# 338. Counting Bits
@pytest.mark.parametrize(
    "n, expected",
    [(2, [0, 1, 1]), (5, [0, 1, 1, 2, 1, 2])],
)
def test_counting_bits(n, expected):
    assert Stream.from_range(0, n + 1).map(int.bit_count).to_list() == expected
