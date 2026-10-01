import xmltodict


def load(stream, include_root=False, **kwargs):
    content = xmltodict.parse(stream, **kwargs)
    if include_root:
        return content
    return next(iter(content.values()))


def dump(data, stream, xml_root="root", **kwargs):
    kwargs.setdefault("pretty", True)
    xmltodict.unparse({xml_root: data}, output=stream, **kwargs)
