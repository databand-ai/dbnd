# © Copyright Databand.ai, an IBM Company 2022

from dbnd.orchestration.cli.utils import PrefixStore


class TestAutoCompleterCache(object):
    def test_flow__simple(self, tmpdir):
        cache = self._target(tmpdir)

        cache.set("foo", "bar")
        cache.index("idx1", "path.to", "foo")
        cache.index("idx1", "path.to.bar", "foo")
        cache.index("idx2", "baz", "foo")
        cache.save()

        cache = self._target(tmpdir)
        cache.load()

        IDX1_1 = [("path.to", "bar")]
        IDX1_2 = [("path.to.bar", "bar")]
        IDX2_1 = [("baz", "bar")]

        def _s(prefix):
            return cache.search(["idx1", "idx2"], prefix)

        assert _s("") == dict(IDX1_1 + IDX1_2)
        assert _s("p") == dict(IDX1_1 + IDX1_2)
        assert _s("path.to") == dict(IDX1_1 + IDX1_2)
        assert _s("path.to.bar") == dict(IDX1_2)
        assert _s("path.to.bar!") == dict()
        assert _s("b") == dict(IDX2_1)
        assert _s("baz") == dict(IDX2_1)
        assert _s("a") == {}

    def _target(self, tmpdir):
        return PrefixStore(str(tmpdir.join("cache")))
