import six


def fix_keys(fix_func, conf):
    """Apply fix_func on every key of a dict"""
    return {fix_func(field): value for field, value in six.iteritems(conf)}


def flat_conf(conf):
    """Flats a configuration iterable to a list of commands ready to concat to"""
    results = []
    for field, value in six.iteritems(conf):
        results.extend(["--conf", field + "=" + value])
    return results
