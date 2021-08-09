import logging

from pytest import fixture

from dbnd._core.task_build.task_signature import build_signature
from targets import Target, target


logger = logging.getLogger(__name__)


@fixture
def databand_test_context():
    # override,
    # so we don't have all these logic running on setup phase
    pass


class TestSignature(object):
    def test_simple(self):
        assert build_signature("t", [("a", "b")])

    def test_dict(self):
        a = build_signature("t", [("a", {1: 2, 2: 3})]).signature
        assert a == build_signature("t", [("a", {2: 3, 1: 2})]).signature

    def test_set(self):
        a = build_signature("t", [("a", {1: {target("a"), target("b")}})]).signature
        assert (
            a
            == build_signature("t", [("a", {1: {target("b"), target("a")}})]).signature
        )
