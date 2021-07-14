import logging
import math

from dbnd._core.errors import DatabandConfigError, friendly_error


logger = logging.getLogger(__name__)

SI_MEMORY_SUFFIXES = ["K", "M", "G", "T", "P", "E"]
ISO_MEMORY_SUFFIXES = ["%si" % s for s in SI_MEMORY_SUFFIXES]
MEMORY_SUFFIXES_WITH_BASE_AND_POWER = [
    (ISO_MEMORY_SUFFIXES, 2, 10),
    (SI_MEMORY_SUFFIXES, 10, 3),
]

LIST_NODES_TIMEOUT_S = 3


class DbndKubeResourcesChecker(object):
    def __init__(self, kube_client, kube_config):
        super(DbndKubeResourcesChecker, self).__init__()
        self.kube_client = kube_client
        self.kube_config = kube_config

    def check_if_resource_request_above_max_capacity(self, condition_message):

        try:
            cpu_request = self.get_cpu_request()
            memory_request = self.get_memory_request()

            if cpu_request or memory_request:
                nodes = self.get_cluster_nodes()
                if not nodes:
                    return

                message_components = []
                if cpu_request:
                    nodes_with_cpu = [
                        (node, float(node.status.capacity["cpu"])) for node in nodes
                    ]
                    max_cpu_node = max(nodes_with_cpu, key=lambda nwc: nwc[1])
                    if cpu_request > max_cpu_node[1]:
                        message_components.append(
                            self.get_resource_above_max_capacity_message(
                                param="request_cpu",
                                param_value=self.kube_config.request_cpu,
                                name="CPUs",
                                max_node=max_cpu_node[0].metadata.name,
                                max_capacity=max_cpu_node[0].status.capacity["cpu"],
                            )
                        )

                if memory_request:
                    nodes_with_memory = [
                        (node, parse_kub_memory_string(node.status.capacity["memory"]))
                        for node in nodes
                    ]
                    max_memory_node = max(nodes_with_memory, key=lambda nwm: nwm[1])
                    if memory_request > max_memory_node[1]:
                        message_components.append(
                            self.get_resource_above_max_capacity_message(
                                param="request_memory",
                                param_value=self.kube_config.request_memory,
                                name="memory",
                                max_node=max_memory_node[0].metadata.name,
                                max_capacity=max_memory_node[0].status.capacity[
                                    "memory"
                                ],
                            )
                        )

                if message_components:
                    raise friendly_error.executor_k8s.kubernetes_pod_unschedulable(
                        condition_message, extra_help="\n".join(message_components)
                    )

        except DatabandConfigError as ex:
            raise ex
        except Exception as ex:
            logger.warning(
                "failed to check if the requested pod resources are above maximum cluster capacity: %s"
                % ex.args
            )

    def get_resource_above_max_capacity_message(self, **kwargs):
        return (
            "Configured {param} ({param_value}) is larger than the node with most {name} in the cluster ({max_node} with {max_capacity}). "
            "Either lower the {param} or add a node with enought {name} to the cluster."
        ).format(**kwargs)

    def get_cpu_request(self):
        """
        https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu
        :return: float value of requested cpu fractions or None
        """
        try:
            if not self.kube_config.request_cpu:
                return None

            if self.kube_config.request_cpu[-1].lower() == "m":
                return int(self.kube_config.request_cpu[:-1]) / 1000.0
            else:
                return float(self.kube_config.request_cpu)
        except ValueError as e:
            raise DatabandConfigError(
                "failed to parse request_cpu %s: %s" % (self.kube_config.request_cpu, e)
            )

    def get_memory_request(self):
        try:
            return parse_kub_memory_string(self.kube_config.request_memory)
        except DatabandConfigError as e:
            raise DatabandConfigError("failed to parse request_memory: %s" % e.args)

    def get_cluster_nodes(self):
        try:
            return self.kube_client.list_node(
                timeout_seconds=LIST_NODES_TIMEOUT_S
            ).items
        except Exception as ex:
            logger.warning(
                "tried to list Kubernetes cluster nodes to generate a better failure message but failed because: %s"
                % ex
            )

        return None


def parse_kub_memory_string(memory_string):
    """
    https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-memory
    :return: float value of requested bytes or None
    """
    if not memory_string:
        return None

    try:
        for suffixes, base, power in MEMORY_SUFFIXES_WITH_BASE_AND_POWER:
            for i, s in enumerate(suffixes, start=1):
                if memory_string.endswith(s) or memory_string.endswith(s.lower()):
                    return float(memory_string[: -len(s)]) * math.pow(base, power * i)
    except ValueError as e:
        raise DatabandConfigError("memory parse failed for %s: %s" % (memory_string, e))

    raise DatabandConfigError(
        "memory parse failed for %s: suffix not recognized" % memory_string
    )
