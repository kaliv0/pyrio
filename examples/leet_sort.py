from pyrio import Stream


# 414. Third Maximum Number
def third_maximum_number(nums: list[int]) -> int:
    ranked = Stream(nums).distinct().reverse().to_list()
    return Stream(ranked).take_nth(2, default=ranked[0]).get()


# third_maximum_number([2, 2, 3, 1]) => 1


# 1491. Average Salary Excluding the Minimum and Maximum Salary
def average_salary_excluding_minmax(salary: list[int]) -> float:
    return Stream(salary).sort().view(1, -1).average()


# average_salary_excluding_minmax([4000, 3000, 1000, 2000]) => 2500.0
