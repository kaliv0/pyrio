from pyrio import Stream


# 70. Climbing Stairs
def climbing_stairs(n: int) -> int:
    return (
        Stream.iterate((1, 1), lambda pair: (pair[1], pair[0] + pair[1]))
        .map(lambda pair: pair[0])
        .take_nth(n)
        .get()
    )


# climbing_stairs(10) => 89


# 1137. N-th Tribonacci Number
def tribonacci(n: int) -> int:
    return (
        Stream.iterate((0, 1, 1), lambda triple: (triple[1], triple[2], sum(triple)))
        .map(lambda triple: triple[0])
        .take_nth(n)
        .get()
    )


# tribonacci(25) => 1389537


# 198. House Robber
def house_robber(nums: list[int]) -> int:
    def step(state, value):
        prev, curr = state
        return curr, max(curr, prev + value)

    return Stream(nums).reduce(step, identity=(0, 0)).map(lambda state: state[1]).get()


# house_robber([2, 7, 9, 3, 1]) => 12


# 746. Min Cost Climbing Stairs
def min_cost_climbing_stairs(cost: list[int]) -> int:
    def step(state, value):
        return state[1], value + min(state[0], state[1])

    return Stream(cost).reduce(step, identity=(0, 0)).map(min).get()


# min_cost_climbing_stairs([10, 15, 20]) => 15


# 53. Maximum Subarray
def maximum_subarray(nums: list[int]) -> int:
    def step(state, value):
        best_ending, best_so_far = state
        best_ending = max(value, best_ending + value)
        return best_ending, max(best_so_far, best_ending)

    return (
        Stream(nums[1:]).reduce(step, identity=(nums[0], nums[0])).map(lambda state: state[1]).get()
    )


# maximum_subarray([-2, 1, -3, 4, -1, 2, 1, -5, 4]) => 6


# 121. Best Time to Buy and Sell Stock
def best_time_to_buy_and_sell_stock(prices: list[int]) -> int:
    def step(state, price):
        lowest, best = state
        return min(lowest, price), max(best, price - lowest)

    return Stream(prices).reduce(step, identity=(prices[0], 0)).map(lambda state: state[1]).get()


# best_time_to_buy_and_sell_stock([7, 1, 5, 3, 6, 4]) => 5


# 118. Pascal's Triangle
def pascals_triangle(num_rows: int) -> list[list[int]]:
    def next_row(row):
        middles = Stream(row).pairwise().map(lambda pair: pair[0] + pair[1]).to_list()
        return [1, *middles, 1]

    return Stream.iterate([1], next_row).limit(num_rows).to_list()


# pascals_triangle(5) => [[1], [1, 1], [1, 2, 1], [1, 3, 3, 1], [1, 4, 6, 4, 1]]


# 62. Unique Paths
def unique_paths(m: int, n: int) -> int:
    return (
        Stream.iterate([1] * n, lambda row: Stream(row).accumulate().to_list())
        .take_nth(m - 1)
        .map(lambda x: x[-1])
        .get()
    )


# unique_paths(3, 7) => 28
