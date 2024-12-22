# inspired from https://www.artima.com/weblogs/viewpost.jsp?thread=101605

METHOD_REGISTRY = {}


# minimalistic implementation, doesn't work with abstract type hints, lambda functions (e.g. passed as predicates) etc
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
        function = getattr(function, "__lastreg__", function)
        name = function.__name__
        multi_method = METHOD_REGISTRY.get(name)
        if multi_method is None:
            multi_method = METHOD_REGISTRY[name] = MultiMethod(name)
        multi_method.register(types, function)
        multi_method.__lastreg__ = function
        return multi_method

    return register
