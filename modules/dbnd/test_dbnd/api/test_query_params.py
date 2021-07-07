from collections import OrderedDict

import pytest

from dbnd.api.query_params import (
    build_query_api_params,
    create_filter_builder,
    create_filters_builder,
)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (dict(), ""),
        (dict(filters=[]), ""),
        (dict(sort="name"), "sort=name"),
        (dict(page_number=6), "page[number]=6"),
        (dict(page_size="6"), "page[size]=6"),
        (
            dict(
                filters=[OrderedDict([("name", "name"), ("op", "do"), ("val", "xyz")])]
            ),
            'filter=[{"name": "name", "op": "do", "val": "xyz"}]',
        ),
        (
            dict(
                filters=[
                    OrderedDict([("name", "name"), ("op", "do"), ("val", "xyz")]),
                    OrderedDict([("name", "other"), ("op", "eq"), ("val", "value")]),
                ]
            ),
            'filter=[{"name": "name", "op": "do", "val": "xyz"}, {"name": "other", "op": "eq", "val": "value"}]',
        ),
        (dict(page_size="6", page_number=6), "page[number]=6&page[size]=6"),
        (
            dict(
                sort="name",
                filters=[OrderedDict([("name", "name"), ("op", "do"), ("val", "xyz")])],
                page_size="6",
                page_number=6,
            ),
            'sort=name&filter=[{"name": "name", "op": "do", "val": "xyz"}]&page[number]=6&page[size]=6',
        ),
    ],
)
def test_build_query_api_params(kwargs, expected):
    assert build_query_api_params(**kwargs) == expected


_mock_build_filters = create_filters_builder(
    job_name=("job_name", "eq"),
    severity=("different_name", "<"),
    alert_type=("type", "eq"),
)


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (dict(), []),
        (
            dict(job_name="job_name"),
            [{"name": "job_name", "op": "eq", "val": "job_name"}],
        ),
        (dict(severity=True), [{"name": "different_name", "op": "<", "val": True}]),
        (dict(severity=False), [{"name": "different_name", "op": "<", "val": False}]),
        (dict(alert_type=6), [{"name": "type", "op": "eq", "val": 6}]),
        (
            dict(job_name="job_name", severity=True, alert_type=6),
            [
                {"name": "job_name", "op": "eq", "val": "job_name"},
                {"name": "different_name", "op": "<", "val": True},
                {"name": "type", "op": "eq", "val": 6},
            ],
        ),
    ],
)
def test_build_filters(kwargs, expected):
    results = _mock_build_filters(**kwargs)
    assert len(results) == len(expected)
    for val in results:
        assert val in expected


_mock_single_filter_builder = create_filter_builder("name", "eq")


@pytest.mark.parametrize(
    "value, expected",
    [("value", [{"name": "name", "op": "eq", "val": "value"}]), (None, [])],
)
def test_build_filter(value, expected):
    assert _mock_single_filter_builder(value) == expected
