# © Copyright Databand.ai, an IBM Company 2022

import copy

from typing import Any, Dict, Optional, Union

import kubernetes.client.models as k8s


class DbndExtendedResources:
    def __init__(
        self,
        requests: Optional[Dict[str, Any]] = None,
        limits: Optional[Dict[str, Any]] = None,
        request_cpu: Optional[Union[str, float]] = None,
        request_memory: Optional[str] = None,
        limit_cpu: Optional[Union[str, float]] = None,
        limit_memory: Optional[str] = None,
    ):
        self.requests = requests or {}
        self.limits = limits or {}
        self.request_cpu = request_cpu
        self.request_memory = request_memory
        self.limit_cpu = limit_cpu
        self.limit_memory = limit_memory

    def to_k8s_client_obj(self) -> k8s.V1ResourceRequirements:
        limits_raw = {"cpu": self.limit_cpu, "memory": self.limit_memory}
        requests_raw = {"cpu": self.request_cpu, "memory": self.request_memory}

        self.limits.update(self.cleanup_empty_values(limits_raw))
        self.requests.update(self.cleanup_empty_values(requests_raw))
        resource_req = k8s.V1ResourceRequirements(
            limits=self.limits, requests=self.requests
        )
        return resource_req

    def cleanup_empty_values(
        self, resource_spec: Dict[str, Optional[Any]]
    ) -> Dict[str, Any]:
        return {k: v for k, v in resource_spec.items() if v}

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        cp_pod = copy.deepcopy(pod)
        resources = self.to_k8s_client_obj()
        cp_pod.spec.containers[0].resources = resources
        return cp_pod
