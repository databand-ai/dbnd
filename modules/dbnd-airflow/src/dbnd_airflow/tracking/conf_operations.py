def fix_keys(fix_func, conf):
    """Apply fix_func on every key of key value iterable"""
    return [(fix_func(field), value) for field, value in conf]


def flat_conf(conf):
    """Flats a configuration iterable to a list of commands ready to concate to   """
    results = []
    for field, value in conf:
        results.extend(["--conf", field + "=" + value])
    return results
