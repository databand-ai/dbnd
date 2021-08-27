import json

import pytest

from kubernetes.client import ApiClient
from mock import Mock

from dbnd._core.errors.friendly_error.executor_k8s import (
    KubernetesRunningPodConditionFailure,
)
from dbnd_docker.kubernetes.kube_dbnd_client import DbndPodCtrl
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


def _check_runtime(pod_v1_resp_raw):
    a = ApiClient()
    response = Mock()
    response.data = json.dumps(pod_v1_resp_raw)
    pod_v1_resp = a.deserialize(response, "V1Pod")

    p = DbndPodCtrl(
        "test",
        "test",
        kube_config=KubernetesEngineConfig(
            check_running_pod_errors=True, container_repository=None
        ),
        kube_client=None,
    )
    return p.check_running_errors(pod_v1_resp)


def test_find_failed_running():
    running_good = {
        u"status": {
            u"qosClass": u"Burstable",
            u"containerStatuses": [
                {
                    u"restartCount": 0,
                    u"name": u"base",
                    u"image": u"testest",
                    u"imageID": u"testest",
                    u"state": {u"running": {u"startedAt": u"2021-01-22T04:54:13Z"}},
                    u"ready": True,
                    u"lastState": {},
                    u"containerID": u"testest",
                }
            ],
            u"podIP": u"testest",
            u"startTime": u"2021-01-22T04:50:52Z",
            u"hostIP": u"testest",
            u"phase": u"Running",
            u"conditions": [
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"Initialized",
                    u"lastTransitionTime": u"2021-01-22T04:50:52Z",
                },
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"Ready",
                    u"lastTransitionTime": u"2021-01-22T04:54:13Z",
                },
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"ContainersReady",
                    u"lastTransitionTime": u"2021-01-22T04:54:13Z",
                },
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"PodScheduled",
                    u"lastTransitionTime": u"2021-01-22T04:50:52Z",
                },
            ],
        },
        u"kind": u"Pod",
        u"spec": {
            u"dnsPolicy": u"ClusterFirst",
            u"securityContext": {},
            u"serviceAccountName": u"default",
            u"schedulerName": u"default-scheduler",
            u"enableServiceLinks": True,
            u"serviceAccount": u"default",
            u"priority": 0,
            u"terminationGracePeriodSeconds": 30,
            u"restartPolicy": u"Never",
            u"affinity": {},
            u"volumes": [],
            u"tolerations": [
                {
                    u"operator": u"Equal",
                    u"value": u"auto-deploy-gcp",
                    u"key": u"env",
                    u"effect": u"NoSchedule",
                },
                {
                    u"operator": u"Equal",
                    u"value": u"true",
                    u"key": u"spot",
                    u"effect": u"NoSchedule",
                },
                {
                    u"operator": u"Exists",
                    u"tolerationSeconds": 300,
                    u"effect": u"NoExecute",
                    u"key": u"node.kubernetes.io/not-ready",
                },
                {
                    u"operator": u"Exists",
                    u"tolerationSeconds": 300,
                    u"effect": u"NoExecute",
                    u"key": u"node.kubernetes.io/unreachable",
                },
            ],
            u"containers": [
                {
                    u"terminationMessagePath": u"/dev/termination-log",
                    u"name": u"base",
                    u"image": u"testest",
                    u"volumeMounts": [],
                    u"terminationMessagePolicy": u"File",
                    u"command": [],
                    u"env": [],
                    u"imagePullPolicy": u"IfNotPresent",
                    u"resources": {u"requests": {u"memory": u"1Gi"}},
                }
            ],
            u"nodeName": u"node_name_node_name",
        },
        u"apiVersion": u"v1",
        u"metadata": {
            u"name": u"dbnd.rund651a794-1",
            u"labels": {},
            u"namespace": u"databand-system",
            u"resourceVersion": u"626525853",
            u"creationTimestamp": u"2021-01-22T04:48:17Z",
            u"annotations": {},
            u"uid": u"b30d83b9-2222-44f4-b24b",
        },
    }

    r = _check_runtime(running_good)
    assert r


def test_raise_error_on_running_bad():
    running_bad = {
        u"status": {
            u"qosClass": u"Burstable",
            u"containerStatuses": [
                {
                    u"restartCount": 0,
                    u"name": u"base",
                    u"image": u"testest",
                    u"imageID": u"testest",
                    u"state": {u"running": {u"startedAt": u"2021-01-22T04:54:13Z"}},
                    u"ready": True,
                    u"lastState": {},
                    u"containerID": u"testest",
                }
            ],
            u"podIP": u"testest",
            u"startTime": u"2021-01-22T04:50:52Z",
            u"hostIP": u"testest",
            u"phase": u"Running",
            u"conditions": [
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"Initialized",
                    u"lastTransitionTime": u"2021-01-22T04:50:52Z",
                },
                {
                    u"status": u"False",
                    u"lastProbeTime": None,
                    u"type": u"Ready",
                    u"lastTransitionTime": u"2021-01-22T05:03:17Z",
                },
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"ContainersReady",
                    u"lastTransitionTime": u"2021-01-22T04:54:13Z",
                },
                {
                    u"status": u"True",
                    u"lastProbeTime": None,
                    u"type": u"PodScheduled",
                    u"lastTransitionTime": u"2021-01-22T04:50:52Z",
                },
            ],
        },
        u"kind": u"Pod",
        u"spec": {
            u"dnsPolicy": u"ClusterFirst",
            u"securityContext": {},
            u"serviceAccountName": u"default",
            u"schedulerName": u"default-scheduler",
            u"enableServiceLinks": True,
            u"serviceAccount": u"default",
            u"nodeSelector": {u"spot": u"true", u"env": u"test"},
            u"priority": 0,
            u"terminationGracePeriodSeconds": 30,
            u"restartPolicy": u"Never",
            u"affinity": {},
            u"volumes": [
                {
                    u"secret": {
                        u"defaultMode": 420,
                        u"secretName": u"default-token-p44mw",
                    },
                    u"name": u"default-token-p44mw",
                }
            ],
            u"tolerations": [],
            u"containers": [
                {
                    u"terminationMessagePath": u"/dev/termination-log",
                    u"name": u"base",
                    u"image": u"testest",
                    u"volumeMounts": [],
                    u"terminationMessagePolicy": u"File",
                    u"command": [],
                    u"env": [],
                    u"imagePullPolicy": u"IfNotPresent",
                    u"resources": {u"requests": {u"memory": u""}},
                }
            ],
            u"nodeName": u"node_name_node_name",
        },
        u"apiVersion": u"v1",
        u"metadata": {},
    }
    with pytest.raises(KubernetesRunningPodConditionFailure):
        r = _check_runtime(running_bad)
