import json


def build_query_api_params(filters=None, page_number=None, page_size=None, sort=None):
    params_map = [
        ("sort", sort),
        ("filter", json.dumps(filters) if filters else None),
        ("page[number]", page_number),
        ("page[size]", page_size),
    ]
    params = [name + "=" + str(value) for name, value in params_map if value]
    return "&".join(params)


def build_filter(name, operator, value):
    return {"name": name, "op": operator, "val": value}


def create_filter_builder(name, operator):
    def inner_func(value):
        if value is not None:
            return [build_filter(name, operator, value)]
        else:
            return []

    return inner_func


def create_filters_builder(**kwargs):
    """Creates a filters builder using a mapping between filter name and the params relevant for
    `create_filter_builder`.
    Thw new builder function receives mapping between the filter name and the value, creates the filter only if the
    value exists and append it to the filters list"""
    filter_builders = {
        arg_name: create_filter_builder(*filter_params)
        for arg_name, filter_params in kwargs.items()
    }

    def filters_builder(**kwargs):
        filters = []
        for name, value in kwargs.items():
            if name not in filter_builders:
                raise KeyError(
                    "filters_builder has not key argument name {name}".format(name=name)
                )

            filters += filter_builders[name](value)

        return filters

    return filters_builder
