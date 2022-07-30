# © Copyright Databand.ai, an IBM Company 2022

import json
import logging
import time

from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.errors.friendly_error.task_execution import failed_to_run_emr_step
from dbnd_aws.credentials import get_boto_emr_client
from dbnd_spark._core.spark_error_parser import parse_spark_log_safe
from targets import target


logger = logging.getLogger(__name__)


class EmrClustersCtrl(object):
    def __init__(self, emr_client):
        super(EmrClustersCtrl, self).__init__()
        self.emr_conn = emr_client  # type: EMR.Client

    def get_cluster(self, cluster_name):
        # type: (str) -> EmrCluster
        if cluster_name.startswith("j-"):
            cluster = self.emr_conn.describe_cluster(ClusterId=cluster_name)
            if not cluster:
                raise DatabandRuntimeError(
                    "Cluster '%s' doesn't exists." % cluster_name
                )
            cluster = cluster["Cluster"]
        else:
            all_clusters = self.emr_conn.list_clusters(
                ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
            )["Clusters"]

            cluster = [c for c in all_clusters if c["Name"] == cluster_name]
            if len(cluster) > 1:
                raise DatabandRuntimeError(
                    "Can't select cluster from %s, please use cluster id (j-..)"
                    % cluster
                )
            elif not cluster:
                available_clusters = [
                    "%s(%s)" % (c["Id"], c["Name"]) for c in all_clusters
                ]
                raise DatabandRuntimeError(
                    "Cluster '%s' doesn't exist in '%s' region, please use cluster id (j-..), Available clusters: %s"
                    % (
                        cluster_name,
                        self.emr_conn.meta.region_name,
                        ",".join(available_clusters),
                    )
                )
            cluster = cluster[0]

        return EmrCluster(emr_conn=self.emr_conn, cluster_id=cluster["Id"])

    @classmethod
    def build_emr_clusters_manager(cls, region_name):
        return cls(emr_client=get_boto_emr_client(region_name=region_name))


class EmrCluster(object):
    def __init__(self, emr_conn, cluster_id):
        super(EmrCluster, self).__init__()
        self.cluster_id = cluster_id
        self.emr_conn = emr_conn

    def get_info(self, info):
        if info:
            return info
        return self.emr_conn.describe_cluster(ClusterId=self.cluster_id)["Cluster"]

    def get_cluster_state(self, info=None):
        return self.get_info(info)["Status"]["State"]

    def get_public_dns(self, info=None):
        return self.get_info(info)["MasterPublicDnsName"]

    def get_master_public_ip(self, cluster_id):
        instances = self.emr_conn.list_instances(
            ClusterId=cluster_id, InstanceGroupTypes=["MASTER"]
        )
        return instances["Instances"][0]["PublicIpAddress"]

    def wait_for_cluster_creation(self):
        self.emr_conn.get_waiter("cluster_running").wait(ClusterId=self.cluster_id)

    def terminate_cluster(self):
        self.emr_conn.terminate_job_flows(JobFlowIds=[self.cluster_id])

    def get_step_info(self, step_id):
        return self.emr_conn.describe_step(ClusterId=self.cluster_id, StepId=step_id)

        # note: this logs will not be available without correct access to the nodes.
        # read more here:
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html

    def get_emr_logs_dict(self, logs_dict):
        result_dict = dict()
        node_dns = self.emr_conn.describe_cluster(ClusterId=self.cluster_id)["Cluster"][
            "MasterPublicDnsName"
        ]
        for (key), log in logs_dict.items():
            try:
                prefix, node, suffix = log
                if node != "<master>":
                    # as for today, there's no easy DNS record for CORE nodes... so filter them from results.
                    continue
                result_dict[key] = prefix + node_dns + "".join(suffix)
            except Exception as e:
                logger.warning("some logs could not retrieved from EMR", e)
                continue
        return result_dict

    def run_spark_submit_step(
        self,
        name: str,
        spark_submit_command,
        action_on_failure: str = "CANCEL_AND_WAIT",
    ):

        step = dict(
            Name=name,
            ActionOnFailure=action_on_failure,
            HadoopJarStep=dict(Jar="command-runner.jar", Args=spark_submit_command),
        )
        response = self.emr_conn.add_job_flow_steps(
            JobFlowId=self.cluster_id, Steps=[step]
        )
        return response["StepIds"][0]

    def wait_for_step_completion(
        self, step_id, status_reporter=None, emr_completion_poll_interval=10
    ):
        status_reporter = status_reporter or self._default_status_reporter
        while True:
            step = self.get_step_info(step_id)
            if "FAILED" == step["Step"]["Status"]["State"]:
                logger.error("Emr step %s has errors", step_id)
                logger.error(step["Step"]["Status"]["FailureDetails"])
                logger.debug(str(step))
                self._raise_error(step)
                return False
            if "CANCELLED" == step["Step"]["Status"]["State"]:
                logger.warning("Emr step %s is cancelled", step_id)
                if "message" in step["Step"]["Status"]["StateChangeReason"]:
                    logger.warning(step["Step"]["Status"]["StateChangeReason"])
                logger.debug(str(step))
                self._raise_error(step)
                return False
            if "COMPLETED" == step["Step"]["Status"]["State"]:
                return True

            status_reporter(step)
            logger.debug(
                "Emr step %s is %s", step, str(step["Step"]["Status"]["State"])
            )
            time.sleep(emr_completion_poll_interval)

    def _raise_error(self, step):
        if "FAILED" == step["Step"]["Status"]["State"]:
            details = step["Step"]["Status"]["FailureDetails"]
            raise failed_to_run_emr_step(
                details["Reason"],
                details["LogFile"],
                self._try_get_errors_from_log(details["LogFile"]),
            )

    def _default_status_reporter(self, step_response):
        logger.info(
            "Batch status: %s", json.dumps(step_response, indent=4, sort_keys=True)
        )

    def _get_errors_from_log(self, path):
        eventual_consistency_sleep_interval = 1
        eventual_consistency_max_sleeps = 30

        for attempt in range(eventual_consistency_max_sleeps):
            log_dump = target(path + "stderr.gz")
            if log_dump.exists():
                return parse_spark_log_safe(log_dump.readlines())

            logging.warning(
                "Emr step logs are not yet available at %s. Waining %i second to retry. Additional attempts: %i\n"
                % (
                    path,
                    eventual_consistency_sleep_interval,
                    eventual_consistency_max_sleeps - attempt,
                )
            )

            time.sleep(eventual_consistency_sleep_interval)

        raise failed_to_run_emr_step("Failed to read log file at %s" % path, None, None)

    def _try_get_errors_from_log(self, path):
        try:
            errors = self._get_errors_from_log(path)
            return errors
        except Exception as e:
            logging.error("Failed to get error from log. Exception: %s" % (e,))
            return ""
