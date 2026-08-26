from pyrio import Stream


# 412. Fizz Buzz
def fizz_buzz(n: int) -> list[str]:
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

    return Stream.from_range(1, n + 1).map(label).to_list()


# fizz_buzz(5) => ["1", "2", "Fizz", "4", "Buzz"]


# 728. Self Dividing Numbers
def self_dividing_numbers(left: int, right: int) -> list[int]:
    def is_self_dividing(num):
        return Stream(str(num)).map(int).all_match(lambda digit: digit != 0 and num % digit == 0)

    return Stream.from_range(left, right + 1).filter(is_self_dividing).to_list()


# self_dividing_numbers(1, 22) => [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 22]


# 509. Fibonacci Number
def fibonacci(n: int) -> list[int]:
    return (
        Stream.iterate((0, 1), lambda pair: (pair[1], pair[0] + pair[1]))
        .map(lambda pair: pair[0])
        .limit(n)
        .to_list()
    )


# fibonacci(8) => [0, 1, 1, 2, 3, 5, 8, 13]


# 1431. Kids With the Greatest Number of Candies
def kids_with_greatest_candies(candies: list[int], extra: int) -> list[bool]:
    greatest = max(candies)
    return Stream(candies).map(lambda count: count + extra >= greatest).to_list()


# kids_with_greatest_candies([2, 3, 5, 1, 3], 3) => [True, True, True, False, True]
