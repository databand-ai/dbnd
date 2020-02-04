import pytest

from dbnd.testing.helpers import run_dbnd_subprocess__dbnd_run


class TestCliFlags(object):
    def test_direct_db(self):
        run_dbnd_subprocess__dbnd_run(["dbnd_sanity_check", "--direct-db"])

        # Should work because direct-db overrides any other tracking configuration
        run_dbnd_subprocess__dbnd_run(
            [
                "dbnd_sanity_check",
                "--set",
                "core.tracker_api=web",
                "--set",
                "core.tracker_url=http://not-exist:54321",
                "--direct-db",
            ]
        )
        pass
