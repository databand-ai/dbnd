import attr


@attr.s(slots=True)
class ValueMetrics(object):
    value_preview = attr.ib()
    data_dimensions = attr.ib()  # type: List
    data_schema = attr.ib()
