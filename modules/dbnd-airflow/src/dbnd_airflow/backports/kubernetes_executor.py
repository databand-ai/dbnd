from distutils.version import LooseVersion

import airflow


if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
    from airflow.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesJobWatcher,
        KubernetesExecutor,
        KubeConfig,
    )
else:
    from airflow.contrib.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesJobWatcher,
        KubernetesExecutor,
        KubeConfig,
    )


def make_safe_label_value(value):
    if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
        from airflow.kubernetes.pod_generator import (
            make_safe_label_value as airflow_make_safe_label_value,
        )

        return airflow_make_safe_label_value(value)

    return AirflowKubernetesScheduler._make_safe_label_value(value)


class BackportedKubernetesJobWatcher(KubernetesJobWatcher):
    def _get_tuple_for_watcher_queue(self, pod_id, state, labels, resource_version):
        if LooseVersion(airflow.version.version) >= LooseVersion("1.10.10"):
            return pod_id, self.namespace, state, labels, resource_version
        return pod_id, state, labels, resource_version
