import logging
import shlex

from typing import Optional

from dbnd._core.errors import ParseParameterError
from dbnd_docker.docker_ctrl import DockerRunCtrl
from dbnd_docker.kubernetes.kube_dbnd_client import DbndPodCtrl
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


logger = logging.getLogger(__name__)


class KubernetesTaskRunCtrl(DockerRunCtrl):
    def __init__(self, **kwargs):
        super(KubernetesTaskRunCtrl, self).__init__(**kwargs)
        self.pod_ctrl = None  # type: Optional[DbndPodCtrl]

    def docker_run(self):

        # dont' describe in local run, do it in remote run
        self.context.settings.system.describe = False
        cmds = shlex.split(self.task.command)

        kubernetes_config = self.task.docker_engine  # type: KubernetesEngineConfig
        if self.task.image:
            # If the image was described in the task itself, it should override the configuration
            try:
                container_repository, container_tag = self.task.image.split(":")
                kubernetes_config = kubernetes_config.clone(
                    container_repository=container_repository,
                    container_tag=container_tag,
                )
            except ValueError as e:
                # ValueError is received when the task.image string is not formatted correctly
                raise ParseParameterError(
                    "Received image %s is not in image format! Expected repo:tag. Exception: %s"
                    % (self.task.image, str(e))
                )
        pod = kubernetes_config.build_pod(cmds=cmds, task_run=self.task_run)
        kube_dbnd = kubernetes_config.build_kube_dbnd()

        self.pod_ctrl = kube_dbnd.get_pod_ctrl_for_pod(pod)
        try:
            self.pod_ctrl.run_pod(pod=pod, task_run=self.task_run)
            if not kubernetes_config.detach_run:
                self.pod_ctrl.delete_pod()
        except Exception:
            # we should not delete pod on Keyboard interrupt
            # external system will take care of that!
            if not kubernetes_config.detach_run:
                self.pod_ctrl.delete_pod()
            raise

        self.pod_ctrl = None

    def on_kill(self):
        if not self.task.docker_engine.detach_run and self.pod_ctrl:
            self.pod_ctrl.delete_pod()
