import logging
import shlex

from typing import Optional

from dbnd_docker.docker_ctrl import DockerRunCtrl
from dbnd_docker.kubernetes.kube_dbnd_client import DbndPodCtrl
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


logger = logging.getLogger(__name__)


class KubernetesTaskRunCtrl(DockerRunCtrl):
    def __init__(self, **kwargs):
        super(KubernetesTaskRunCtrl, self).__init__(**kwargs)
        self.pod_ctrl = None  # type: Optional[DbndPodCtrl]

    @property
    def kubernetes_config(self):
        # type: () -> KubernetesEngineConfig
        return self.task.docker_engine

    def docker_run(self):

        # dont' describe in local run, do it in remote run
        self.context.settings.system.describe = False
        cmds = shlex.split(self.task.command)

        pod = self.kubernetes_config.build_pod(cmds=cmds, task_run=self.task_run)
        kube_dbnd = self.kubernetes_config.build_kube_dbnd()

        self.pod_ctrl = kube_dbnd.get_pod_ctrl_for_pod(pod)
        try:
            self.pod_ctrl.run_pod(pod=pod, task_run=self.task_run)
            if not self.kubernetes_config.detach_run:
                self.pod_ctrl.delete_pod()
        except Exception:
            # we should not delete pod on Keyboard interrupt
            # external system will take care of that!
            if not self.kubernetes_config.detach_run:
                self.pod_ctrl.delete_pod()
            raise

        self.pod_ctrl = None

    def on_kill(self):
        if not self.kubernetes_config.detach_run and self.pod_ctrl:
            self.pod_ctrl.delete_pod()
