from dbnd_airflow.constants import AIRFLOW_ABOVE_10


def serialize_pod(pod, engine_config):
    """
    @param pod: Airflow pod definition
    @param engine_config: KubernetesEngineConfig
    @return:
    """
    if AIRFLOW_ABOVE_10:
        pod = pod.to_v1_kubernetes_pod()

        from airflow.kubernetes.kube_client import get_kube_client

        # airflow>1.10.10 uses official kubernetes client (https://github.com/kubernetes-client/python/)
        # to create pod json request instead of custom SimplePodRequest class
        kube_client = get_kube_client(in_cluster=engine_config.in_cluster)
        return kube_client.api_client.sanitize_for_serialization(pod)

    from airflow.contrib.kubernetes.kubernetes_request_factory.pod_request_factory import (
        SimplePodRequestFactory as AirflowSimplePodRequestFactory,
    )

    return AirflowSimplePodRequestFactory().create(pod)
