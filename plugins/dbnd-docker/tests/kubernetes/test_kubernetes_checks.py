# © Copyright Databand.ai, an IBM Company 2022

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
        "status": {
            "qosClass": "Burstable",
            "containerStatuses": [
                {
                    "restartCount": 0,
                    "name": "base",
                    "image": "testest",
                    "imageID": "testest",
                    "state": {"running": {"startedAt": "2021-01-22T04:54:13Z"}},
                    "ready": True,
                    "lastState": {},
                    "containerID": "testest",
                }
            ],
            "podIP": "testest",
            "startTime": "2021-01-22T04:50:52Z",
            "hostIP": "testest",
            "phase": "Running",
            "conditions": [
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "Initialized",
                    "lastTransitionTime": "2021-01-22T04:50:52Z",
                },
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "Ready",
                    "lastTransitionTime": "2021-01-22T04:54:13Z",
                },
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "ContainersReady",
                    "lastTransitionTime": "2021-01-22T04:54:13Z",
                },
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "PodScheduled",
                    "lastTransitionTime": "2021-01-22T04:50:52Z",
                },
            ],
        },
        "kind": "Pod",
        "spec": {
            "dnsPolicy": "ClusterFirst",
            "securityContext": {},
            "serviceAccountName": "default",
            "schedulerName": "default-scheduler",
            "enableServiceLinks": True,
            "serviceAccount": "default",
            "priority": 0,
            "terminationGracePeriodSeconds": 30,
            "restartPolicy": "Never",
            "affinity": {},
            "volumes": [],
            "tolerations": [
                {
                    "operator": "Equal",
                    "value": "auto-deploy-gcp",
                    "key": "env",
                    "effect": "NoSchedule",
                },
                {
                    "operator": "Equal",
                    "value": "true",
                    "key": "spot",
                    "effect": "NoSchedule",
                },
                {
                    "operator": "Exists",
                    "tolerationSeconds": 300,
                    "effect": "NoExecute",
                    "key": "node.kubernetes.io/not-ready",
                },
                {
                    "operator": "Exists",
                    "tolerationSeconds": 300,
                    "effect": "NoExecute",
                    "key": "node.kubernetes.io/unreachable",
                },
            ],
            "containers": [
                {
                    "terminationMessagePath": "/dev/termination-log",
                    "name": "base",
                    "image": "testest",
                    "volumeMounts": [],
                    "terminationMessagePolicy": "File",
                    "command": [],
                    "env": [],
                    "imagePullPolicy": "IfNotPresent",
                    "resources": {"requests": {"memory": "1Gi"}},
                }
            ],
            "nodeName": "node_name_node_name",
        },
        "apiVersion": "v1",
        "metadata": {
            "name": "dbnd.rund651a794-1",
            "labels": {},
            "namespace": "databand-system",
            "resourceVersion": "626525853",
            "creationTimestamp": "2021-01-22T04:48:17Z",
            "annotations": {},
            "uid": "b30d83b9-2222-44f4-b24b",
        },
    }

    r = _check_runtime(running_good)
    assert r


def test_raise_error_on_running_bad():
    running_bad = {
        "status": {
            "qosClass": "Burstable",
            "containerStatuses": [
                {
                    "restartCount": 0,
                    "name": "base",
                    "image": "testest",
                    "imageID": "testest",
                    "state": {"running": {"startedAt": "2021-01-22T04:54:13Z"}},
                    "ready": True,
                    "lastState": {},
                    "containerID": "testest",
                }
            ],
            "podIP": "testest",
            "startTime": "2021-01-22T04:50:52Z",
            "hostIP": "testest",
            "phase": "Running",
            "conditions": [
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "Initialized",
                    "lastTransitionTime": "2021-01-22T04:50:52Z",
                },
                {
                    "status": "False",
                    "lastProbeTime": None,
                    "type": "Ready",
                    "lastTransitionTime": "2021-01-22T05:03:17Z",
                },
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "ContainersReady",
                    "lastTransitionTime": "2021-01-22T04:54:13Z",
                },
                {
                    "status": "True",
                    "lastProbeTime": None,
                    "type": "PodScheduled",
                    "lastTransitionTime": "2021-01-22T04:50:52Z",
                },
            ],
        },
        "kind": "Pod",
        "spec": {
            "dnsPolicy": "ClusterFirst",
            "securityContext": {},
            "serviceAccountName": "default",
            "schedulerName": "default-scheduler",
            "enableServiceLinks": True,
            "serviceAccount": "default",
            "nodeSelector": {"spot": "true", "env": "test"},
            "priority": 0,
            "terminationGracePeriodSeconds": 30,
            "restartPolicy": "Never",
            "affinity": {},
            "volumes": [
                {
                    "secret": {"defaultMode": 420, "secretName": "default-token-p44mw"},
                    "name": "default-token-p44mw",
                }
            ],
            "tolerations": [],
            "containers": [
                {
                    "terminationMessagePath": "/dev/termination-log",
                    "name": "base",
                    "image": "testest",
                    "volumeMounts": [],
                    "terminationMessagePolicy": "File",
                    "command": [],
                    "env": [],
                    "imagePullPolicy": "IfNotPresent",
                    "resources": {"requests": {"memory": ""}},
                }
            ],
            "nodeName": "node_name_node_name",
        },
        "apiVersion": "v1",
        "metadata": {},
    }
    with pytest.raises(KubernetesRunningPodConditionFailure):
        _check_runtime(running_bad)
