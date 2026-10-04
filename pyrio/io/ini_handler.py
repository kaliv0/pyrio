import configparser

ConfigParserError = configparser.ParsingError


def load(file_handler, **kwargs):
    parser = configparser.ConfigParser(**kwargs)
    parser.optionxform = str  # keep key case (default lowercases)
    parser.read_file(file_handler)

    # dotted sections -> nested dicts:
    #   [first]              -> {"first": {...}}
    #   [first.still-second] -> {"first": {"still-second": {...}}}
    result = {}
    for section in parser.sections():
        node = result
        for part in section.split("."):
            node = node.setdefault(part, {})
        # _sections: only keys written in this section (no DEFAULT leak)
        for k, v in parser._sections.get(section, {}).items():
            node[k] = _parse_val(v)

    # [root] is FileStream write convention (like XML), unwrap for callers
    return result["root"] if list(result) == ["root"] else result


def dump(data, file_handler, **kwargs):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    def walk(obj, path):
        # scalars -> options on this section, nested dicts -> dotted child sections
        # e.g. {"Email": {"primary": "x"}} under root -> [root.Email] primary = x
        scalars = {k: _fmt_val(v) for k, v in obj.items() if not isinstance(v, dict)}
        if scalars:
            parser[path] = scalars
        for k, v in obj.items():
            if isinstance(v, dict):
                walk(v, f"{path}.{k}")

    # always wrap under [root] so top-level scalars have a section
    walk(data, "root")
    parser.write(file_handler, space_around_delimiters=kwargs.get("space_around_delimiters", True))


def _parse_val(v):
    # INI has no arrays; "a, b, c" -> list on load
    return [p.strip() for p in v.split(",")] if "," in v else v


def _fmt_val(v):
    if isinstance(v, list):
        return ", ".join(map(str, v))
    return "" if v is None else str(v)
