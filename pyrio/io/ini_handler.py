import configparser

ConfigParserError = configparser.ParsingError


def load(
    file_handler,
    *,
    merge_defaults=True,
    nest_sections=False,
    split_lists=False,
    unwrap_root=False,
    **kwargs,
):
    parser = _get_parser(**kwargs)
    parser.read_file(file_handler)
    defaults_map = {k: _parse_value(v, split_lists) for k, v in parser.defaults().items()}

    if not merge_defaults:
        # structural view: section-only keys (no DEFAULT inheritance via items())
        parser[parser.default_section].clear()

    result = {}
    for section in parser.sections():
        opts = {k: _parse_value(v, split_lists) for k, v in parser.items(section)}
        if nest_sections:
            # dotted sections -> nested dicts:
            #   [first]              -> {"first": {...}}
            #   [first.still-second] -> {"first": {"still-second": {...}}}
            node = result
            for part in section.split("."):
                node = node.setdefault(part, {})
            node.update(opts)
        else:
            result[section] = opts

    if not merge_defaults and defaults_map:
        result = {parser.default_section: defaults_map, **result}

    # [root] is FileStream write convention (like XML), unwrap for callers
    if unwrap_root and list(result) == ["root"]:
        return result["root"]
    return result


def dump(
    data,
    file_handler,
    *,
    nest_sections=False,
    join_lists=True,
    wrap_root=False,
    space_around_delimiters=True,
    **kwargs,
):
    parser = _get_parser(**kwargs)
    if wrap_root:
        # wrap under [root] so top-level scalars have a section
        _walk(data, "root", parser, join_lists)
    elif nest_sections:
        for section, opts in data.items():
            _walk(_validate_options(opts, section), section, parser, join_lists)
    else:
        for section, opts in data.items():
            parser[section] = {
                k: _format_value(v, join_lists) for k, v in _validate_options(opts, section).items()
            }
    parser.write(file_handler, space_around_delimiters)


def _walk(obj, path, parser, join_lists):
    # scalars -> options on this section, nested dicts -> dotted child sections
    # e.g. {"Email": {"primary": "x"}} under root -> [root.Email] primary = x
    scalars = {k: _format_value(v, join_lists) for k, v in obj.items() if not isinstance(v, dict)}
    if scalars:
        parser[path] = scalars
    for k, v in obj.items():
        if isinstance(v, dict):
            _walk(v, f"{path}.{k}", parser, join_lists)


def _parse_value(val, split_lists=False):
    # INI has no arrays; "a, b, c" -> list on load when split_lists=True
    if split_lists and "," in val:
        return [p.strip() for p in val.split(",")]
    return val


def _format_value(val, join_lists=True):
    if isinstance(val, list) and join_lists:
        return ", ".join(map(str, val))
    return "" if val is None else str(val)


def _get_parser(**kwargs):
    parser = configparser.ConfigParser(**kwargs)
    parser.optionxform = str  # keep key case (default lowercases)
    return parser


def _validate_options(opts, section):
    if not isinstance(opts, dict):
        raise TypeError(f"INI section {section!r} must map to a dict of options, got {type(opts).__name__}")
    return opts
