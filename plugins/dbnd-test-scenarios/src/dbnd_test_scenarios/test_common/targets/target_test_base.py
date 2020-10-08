import pytest

from targets import DataTarget, target


class TargetTestBase(object):
    @pytest.fixture(autouse=True)
    def _set_temp_dir(self, tmpdir):
        self.tmpdir = tmpdir

    def target(self, *args, **kwargs):
        # type: (...) -> DataTarget
        return target(str(self.tmpdir), *args, **kwargs)
