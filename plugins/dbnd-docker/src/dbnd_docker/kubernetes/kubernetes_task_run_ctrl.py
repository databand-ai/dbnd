import logging
import shlex

from dbnd._core.errors import DatabandRuntimeError
from dbnd_docker.docker_ctrl import DockerRunCtrl
from dbnd_docker.kubernetes.kube_dbnd_client import DbndKubernetesClient
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


logger = logging.getLogger(__name__)


class KubernetesTaskRunCtrl(DockerRunCtrl):
    def __init__(self, **kwargs):
        super(KubernetesTaskRunCtrl, self).__init__(**kwargs)
        self.pod = None
        self.kube_dbnd = None  # type: DbndKubernetesClient

    @property
    def kubernetes_config(self):
        # type: () -> KubernetesEngineConfig
        return self.task.docker_engine

    def docker_run(self):
        from airflow.utils.state import State

        # dont' describe in local run, do it in remote run
        self.context.settings.system.describe = False
        kc = self.kubernetes_config
        self.kube_dbnd = kc.build_kube_dbnd()

        cmds = shlex.split(self.task.command)
        self.pod = self.kubernetes_config.build_pod(cmds=cmds, task_run=self.task_run)

        task_run_async = self.kubernetes_config.task_run_async

        pod_ctrl = None
        try:
            pod_ctrl = self.kube_dbnd.run_pod(
                pod=self.pod, run_async=task_run_async, task_run=self.task_run
            )
            final_state = pod_ctrl.get_airflow_state()
        except KeyboardInterrupt as ex:
            logger.info(
                "Keyboard interrupt: stopping execution and deleting Kubernetes driver pod"
            )
            if pod_ctrl:
                pod_ctrl.delete_pod()
            raise ex
        finally:
            if not task_run_async and pod_ctrl:
                pod_ctrl.delete_pod()
            self.kube_dbnd = None

        if task_run_async:
            return

        if final_state != State.SUCCESS:
            raise DatabandRuntimeError(
                "Pod returned a failure: {state}".format(state=final_state)
            )

    def on_kill(self):

        if self.kube_dbnd and self.pod:
            self.kube_dbnd.get_pod_ctrl(self.pod.name, self.pod.namespace).delete_pod()
            self.kube_dbnd = None
