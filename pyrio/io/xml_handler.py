import xmltodict


def load(file_handler, include_root=False, **kwargs):
    content = xmltodict.parse(file_handler, **kwargs)
    if include_root:
        return content
    return next(iter(content.values()))


def dump(data, file_handler, xml_root="root", **kwargs):
    kwargs.setdefault("pretty", True)
    xmltodict.unparse({xml_root: data}, output=file_handler, **kwargs)
