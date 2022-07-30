# © Copyright Databand.ai, an IBM Company 2022

import sys

import pytest

from dbnd._core.utils.platform import windows_compatible_mode
from dbnd.testing.helpers import run_test_notebook
from dbnd_test_scenarios import scenario_path


class TestJupyter(object):
    @pytest.mark.skipif(
        not sys.version_info >= (3, 6) or windows_compatible_mode,
        reason="requires python36 and nvd3",
    )
    @pytest.mark.skip("temporaly skipped due to ipython version")
    def test_simple36_py(self):
        run_test_notebook(scenario_path("jupyter", "simple-py36.ipynb"))
