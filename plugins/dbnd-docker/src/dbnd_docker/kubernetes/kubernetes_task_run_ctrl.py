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

        try:
            self.kube_dbnd.run_pod(
                self.pod, run_async=task_run_async, task_run=self.task_run
            )
            final_state = self.kube_dbnd.get_pod_state(
                self.pod.name, self.pod.namespace
            )
        except KeyboardInterrupt as ex:
            logger.info(
                "Keyboard interrupt: stopping execution and deleting Kubernetes driver pod"
            )
            self.on_kill()
            raise ex
        finally:
            if not task_run_async:
                self.on_kill()
            self.kube_dbnd = None

        if task_run_async:
            return

        if final_state != State.SUCCESS:
            raise DatabandRuntimeError(
                "Pod returned a failure: {state}".format(state=final_state)
            )

    def on_kill(self):
        from airflow.utils.state import State

        if self.kube_dbnd and self.pod:
            if self.kubernetes_config.delete_pods and not (
                self.kubernetes_config.keep_failed_pods
                and self.kube_dbnd.get_pod_state(self.pod.name, self.pod.namespace)
                == State.FAILED
            ):
                logger.error("Deleting pod %s", self.pod.name)
                self.kube_dbnd.delete_pod(self.pod)

            self.kube_dbnd = None
