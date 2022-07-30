# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.utils.basics import signal_utils
from dbnd._core.utils.basics.signal_utils import sigquit_handler__dump_stack


class TestSignalUtils(object):
    def test_signal_quit(self, tmpdir):
        org = signal_utils.SIGQUIT_DUMP_DIR
        try:
            signal_utils.SIGQUIT_DUMP_DIR = tmpdir
            sigquit_handler__dump_stack(-3, None)

        finally:
            signal_utils.SIGQUIT_DUMP_DIR = org
