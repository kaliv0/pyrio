from collections import UserDict


class AliasDict(UserDict):
    VAL_DICT = {}

    def add_alias(self, key, alias):
        self.VAL_DICT[alias] = key

    # TODO: iterkeys() ?? -> add itervals and iteritems
    def get_keys(self):
        return {*self.keys(), *self.VAL_DICT.keys()}  # TODO: generator/iter vs set?

    def __missing__(self, key):
        try:
            return super().__getitem__(self.VAL_DICT[key])
        except AttributeError:
            raise KeyError(key)

    # def __getitem__(self, key):
    #     try:
    #         return super().__getitem__(key)
    #     except KeyError:
    #         pass
    #     try:
    #         return super().__getitem__(self.VAL_DICT[key])
    #     except KeyError:
    #         pass
    #     raise KeyError(key)

    def __setitem__(self, key, value):
        try:
            key = self.VAL_DICT[key]
        except KeyError:
            pass
        super().__setitem__(key, value)
