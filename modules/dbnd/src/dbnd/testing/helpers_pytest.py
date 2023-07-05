# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.utils.platform import windows_compatible_mode


skip_on_windows = pytest.mark.skipif(
    windows_compatible_mode, reason="not supported on Windows OS"
)
