def filter_dict_remove_false_values(d):
    return {k: v for k, v in d.items() if v is not False}
