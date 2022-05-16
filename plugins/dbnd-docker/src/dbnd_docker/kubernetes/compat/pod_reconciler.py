# Vendorized from Apache Airflow
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# This file has been modified by databand.ai to support dbnd orchestration.
"""
This is included in Airflow 2.x PodGenerator object, and we can switch to it once we drop Airflow 1.x
"""
import copy

from typing import List, Optional

from kubernetes.client import models as k8s


def reconcile_pods(base_pod: k8s.V1Pod, client_pod: Optional[k8s.V1Pod]) -> k8s.V1Pod:
    """
    :param base_pod: has the base attributes which are overwritten if they exist
        in the client pod and remain if they do not exist in the client_pod
    :type base_pod: k8s.V1Pod
    :param client_pod: the pod that the client wants to create.
    :type client_pod: k8s.V1Pod
    :return: the merged pods

    This can't be done recursively as certain fields some overwritten, and some concatenated.
    """
    if client_pod is None:
        return base_pod

    client_pod_cp = copy.deepcopy(client_pod)
    client_pod_cp.spec = reconcile_specs(base_pod.spec, client_pod_cp.spec)
    client_pod_cp.metadata = reconcile_metadata(
        base_pod.metadata, client_pod_cp.metadata
    )
    client_pod_cp = merge_objects(base_pod, client_pod_cp)

    return client_pod_cp


def reconcile_metadata(base_meta, client_meta):
    """
    Merge kubernetes Metadata objects
    :param base_meta: has the base attributes which are overwritten if they exist
        in the client_meta and remain if they do not exist in the client_meta
    :type base_meta: k8s.V1ObjectMeta
    :param client_meta: the spec that the client wants to create.
    :type client_meta: k8s.V1ObjectMeta
    :return: the merged specs
    """
    if base_meta and not client_meta:
        return base_meta
    if not base_meta and client_meta:
        return client_meta
    elif client_meta and base_meta:
        client_meta.labels = merge_objects(base_meta.labels, client_meta.labels)
        client_meta.annotations = merge_objects(
            base_meta.annotations, client_meta.annotations
        )
        extend_object_field(base_meta, client_meta, "managed_fields")
        extend_object_field(base_meta, client_meta, "finalizers")
        extend_object_field(base_meta, client_meta, "owner_references")
        return merge_objects(base_meta, client_meta)

    return None


def reconcile_specs(
    base_spec: Optional[k8s.V1PodSpec], client_spec: Optional[k8s.V1PodSpec]
) -> Optional[k8s.V1PodSpec]:
    """
    :param base_spec: has the base attributes which are overwritten if they exist
        in the client_spec and remain if they do not exist in the client_spec
    :type base_spec: k8s.V1PodSpec
    :param client_spec: the spec that the client wants to create.
    :type client_spec: k8s.V1PodSpec
    :return: the merged specs
    """
    if base_spec and not client_spec:
        return base_spec
    if not base_spec and client_spec:
        return client_spec
    elif client_spec and base_spec:
        client_spec.containers = reconcile_containers(
            base_spec.containers, client_spec.containers
        )
        merged_spec = extend_object_field(base_spec, client_spec, "init_containers")
        merged_spec = extend_object_field(base_spec, merged_spec, "volumes")
        return merge_objects(base_spec, merged_spec)

    return None


def reconcile_containers(
    base_containers: List[k8s.V1Container], client_containers: List[k8s.V1Container]
) -> List[k8s.V1Container]:
    """
    :param base_containers: has the base attributes which are overwritten if they exist
        in the client_containers and remain if they do not exist in the client_containers
    :type base_containers: List[k8s.V1Container]
    :param client_containers: the containers that the client wants to create.
    :type client_containers: List[k8s.V1Container]
    :return: the merged containers

    The runs recursively over the list of containers.
    """
    if not base_containers:
        return client_containers
    if not client_containers:
        return base_containers

    client_container = client_containers[0]
    base_container = base_containers[0]
    client_container = extend_object_field(
        base_container, client_container, "volume_mounts"
    )
    client_container = extend_object_field(base_container, client_container, "env")
    client_container = extend_object_field(base_container, client_container, "env_from")
    client_container = extend_object_field(base_container, client_container, "ports")
    client_container = extend_object_field(
        base_container, client_container, "volume_devices"
    )
    client_container = merge_objects(base_container, client_container)

    return [client_container] + reconcile_containers(
        base_containers[1:], client_containers[1:]
    )


def merge_objects(base_obj, client_obj):
    """
    :param base_obj: has the base attributes which are overwritten if they exist
        in the client_obj and remain if they do not exist in the client_obj
    :param client_obj: the object that the client wants to create.
    :return: the merged objects
    """
    if not base_obj:
        return client_obj
    if not client_obj:
        return base_obj

    client_obj_cp = copy.deepcopy(client_obj)

    if isinstance(base_obj, dict) and isinstance(client_obj_cp, dict):
        base_obj_cp = copy.deepcopy(base_obj)
        base_obj_cp.update(client_obj_cp)
        return base_obj_cp

    for base_key in base_obj.to_dict().keys():
        base_val = getattr(base_obj, base_key, None)
        if not getattr(client_obj, base_key, None) and base_val:
            if not isinstance(client_obj_cp, dict):
                setattr(client_obj_cp, base_key, base_val)
            else:
                client_obj_cp[base_key] = base_val
    return client_obj_cp


def extend_object_field(base_obj, client_obj, field_name):
    """
    :param base_obj: an object which has a property `field_name` that is a list
    :param client_obj: an object which has a property `field_name` that is a list.
        A copy of this object is returned with `field_name` modified
    :param field_name: the name of the list field
    :type field_name: str
    :return: the client_obj with the property `field_name` being the two properties appended
    """
    client_obj_cp = copy.deepcopy(client_obj)
    base_obj_field = getattr(base_obj, field_name, None)
    client_obj_field = getattr(client_obj, field_name, None)

    if (not isinstance(base_obj_field, list) and base_obj_field is not None) or (
        not isinstance(client_obj_field, list) and client_obj_field is not None
    ):
        raise ValueError("The chosen field must be a list.")

    if not base_obj_field:
        return client_obj_cp
    if not client_obj_field:
        setattr(client_obj_cp, field_name, base_obj_field)
        return client_obj_cp

    appended_fields = base_obj_field + client_obj_field
    setattr(client_obj_cp, field_name, appended_fields)
    return client_obj_cp
