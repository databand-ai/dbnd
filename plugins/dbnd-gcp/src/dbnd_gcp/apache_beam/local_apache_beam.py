import logging

from dbnd._core.utils.better_subprocess import run_cmd
from dbnd_gcp.apache_beam import ApacheBeamJobCtrl


logger = logging.getLogger(__name__)


class LocalApacheBeamJobCtrl(ApacheBeamJobCtrl):
    def _get_base_options(self):
        options = super(LocalApacheBeamJobCtrl, self)._get_base_options()
        options.setdefault("runner", "DirectRunner")

        return options

    def _run_cmd(self, cmd):
        run_cmd(cmd)
