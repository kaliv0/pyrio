import pickle
from collections.abc import Mapping


def load(file_handler, *, trust_pickle=False, **kwargs):
    if not trust_pickle:
        raise ValueError("Cannot load pickle without trust_pickle=True")

    data = pickle.load(file_handler, **kwargs)
    if not isinstance(data, (Mapping, list, tuple)):
        return (data,)
    return data
