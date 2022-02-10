from six.moves.builtins import str


def parse_bool(s):
    if s is None:
        return False

    val = str(s).lower().strip()
    if "#" in val:
        val = val.split("#")[0].strip()
    if val.lower() in ("t", "true", "yes", "1"):
        return True
    elif val.lower() in ("f", "false", "no", "0"):
        return False
    else:
        raise ValueError("Can't parse '%s' as boolean" % s)


def indent(string, prefix):
    """a backward compatible textwrap.indent replacement"""
    if not string:
        return ""

    return "\n".join(prefix + l for l in string.splitlines())
