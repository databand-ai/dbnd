# © Copyright Databand.ai, an IBM Company 2022

import typing


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun


class RunCtrl(object):
    def __init__(self, run):
        # type: (DatabandRun)-> None
        super(RunCtrl, self).__init__()

        self.run = run

    @property
    def context(self):
        return self.run.context

    @property
    def settings(self):
        return self.context.settings
