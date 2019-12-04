import sys

import pytest
import six

from dbnd._core.utils.platform import windows_compatible_mode
from dbnd.testing.helpers import run_test_notebook
from test_dbnd.scenarios import scenario_path


class TestJupyter(object):
    @pytest.mark.skipif(not six.PY2, reason="requires python2")
    def test_simple27_py(self):
        run_test_notebook(scenario_path("jupyter", "simple-py27.ipynb"))

    @pytest.mark.skipif(
        not sys.version_info >= (3, 6) or windows_compatible_mode,
        reason="requires python36 and nvd3",
    )
    def test_simple36_py(self):
        run_test_notebook(scenario_path("jupyter", "simple-py36.ipynb"))
