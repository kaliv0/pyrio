# inspired from https://www.artima.com/weblogs/viewpost.jsp?thread=101605
METHOD_REGISTRY = {}


class MultiMethod:
    def __init__(self, name):
        self.name = name
        self.typemap = {}

    def __call__(self, cls, *args):
        types = tuple(arg.__class__ for arg in args)
        function = self.typemap.get(types)
        if function is None:
            raise TypeError("No match found")
        return function(cls, *args)

    def register(self, types, function):
        if types in self.typemap:
            raise TypeError("Duplicate registration")
        self.typemap[types] = function


def dispatch(*types):
    def register(function):
        name = function.__name__
        multi_method = METHOD_REGISTRY.get(name)
        if multi_method is None:
            multi_method = METHOD_REGISTRY[name] = MultiMethod(name)
        multi_method.register(types, function)
        return multi_method

    return register