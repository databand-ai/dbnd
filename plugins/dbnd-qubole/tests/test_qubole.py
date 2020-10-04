import os

import pytest

from mock import MagicMock, Mock, patch

import dbnd_qubole

from dbnd import PythonTask, Task
from dbnd_qubole import QuboleConfig
from dbnd_qubole.qubole import QuboleCtrl


UI_URL = "https://api.qubole.com"
COMMAND_ID = "1234"


class TestQuboleCtrl(object):
    def test_log_url_generation(self):
        qubole_config = Mock(QuboleConfig)
        qubole_config.configure_mock(ui_url=UI_URL)
        qubole_ctrl = Mock(QuboleCtrl)
        qubole_ctrl.configure_mock(
            qubole_config=qubole_config, _get_url=QuboleCtrl._get_url
        )
        a = qubole_ctrl._get_url(qubole_ctrl, "1234")
        assert a == "{UI_URL}/v2/analyze?command_id={COMMAND_ID}".format(
            UI_URL=UI_URL, COMMAND_ID=COMMAND_ID
        )
