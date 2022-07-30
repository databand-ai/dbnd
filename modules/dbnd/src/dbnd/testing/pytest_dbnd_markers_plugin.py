# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import typing

import pytest


OnRequestMarker = typing.NamedTuple(
    "OnRequestMarker", [("name", str), ("flag", str), ("description", str)]
)

ON_REQUEST_TEST_TYPES = [
    OnRequestMarker("slow", "--run-slow", "run more than 30 seconds"),
    # clouds
    OnRequestMarker("gcp", "--run-gcp", "require configured gcp"),
    OnRequestMarker("gcp_k8s", "--run-k8s-gcp", "require configured gcp k8s cluster"),
    OnRequestMarker("aws", "--run-aws", "require configured aws"),
    OnRequestMarker("emr", "--run-emr", "require configured emr"),
    OnRequestMarker("livy", "--run-livy", "require configured livy"),
    OnRequestMarker("awsbatch", "--run-awsbatch", "require configured aws batch"),
    OnRequestMarker("azure", "--run-azure", "require configured azure"),
    # tools
    OnRequestMarker("spark", "--run-spark", "require configured local spark"),
    OnRequestMarker("beam", "--run-beam", "require configured local beam"),
    OnRequestMarker("docker", "--run-docker", "require configured local docker"),
]


def pytest_addoption(parser):
    for m in ON_REQUEST_TEST_TYPES:
        parser.addoption(
            m.flag,
            action="store_true",
            default=False,
            help="run tests that %s" % m.description,
        )
    parser.addoption(
        "--run-all",
        action="store_true",
        default=False,
        help="run all tests regardless mark(slow, gcp, aws..)",
    )


def pytest_collection_modifyitems(config, items):
    skips = {
        m.name: pytest.mark.skip(reason="need --run-%s option to run" % m.name)
        for m in ON_REQUEST_TEST_TYPES
    }

    if config.getoption("--run-all"):
        return

    disabled = {m.name: not config.getoption(m.flag) for m in ON_REQUEST_TEST_TYPES}
    for item in items:
        for m in ON_REQUEST_TEST_TYPES:
            if m.name in item.keywords and disabled[m.name]:
                item.add_marker(skips[m.name])
