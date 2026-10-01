def load(stream, **kwargs):
    import yaml

    if (parse_float := kwargs.pop("parse_float", None)) is None:
        return yaml.safe_load(stream, **kwargs)

    class Loader(yaml.SafeLoader):
        pass

    Loader.add_constructor(
        "tag:yaml.org,2002:float",
        lambda loader, node: parse_float(loader.construct_scalar(node)),
    )
    return yaml.load(stream, Loader=Loader)
