from stream.stream import Stream

if __name__ == '__main__':
    # result = (Stream([1, 2, 3, 4, 5, 6])
    #           .filter(lambda x: x % 2 == 0)
    #           .map(lambda x: '#' * x)
    #           .filter(lambda x: len(x) <= 4)
    #           .to_list())

    result = (Stream([1, 2, 3, 4, 5, 6])
              .filter(lambda x: x % 2 == 0)
              .map(lambda x: x * 3)
              .reduce(lambda x, y: x + y, 0))

    print(result)


