# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.utils.http.endpoint import Endpoint
from dbnd_aws.emr.emr_cluster import EmrClustersCtrl
from dbnd_spark.livy.livy_spark import _LivySparkCtrl
from dbnd_spark.spark_ctrl import SparkCtrl


logger = logging.getLogger(__name__)


class EmrCtrl(SparkCtrl):
    def __init__(self, task_run):
        super(EmrCtrl, self).__init__(task_run=task_run)

        self.emr_config = task_run.task.spark_engine

        self.clusters = EmrClustersCtrl.build_emr_clusters_manager(
            region_name=self.emr_config.region
        )
        self.emr_cluster = self.clusters.get_cluster(self.emr_config.cluster)

    def _run_spark_submit(self, file, jars):
        raise NotImplementedError()

    def _set_spark_external_urls(self):
        self.task_run.set_external_resource_urls(
            self.emr_cluster.get_emr_logs_dict(self.spark_application_logs)
        )

    def run_pyspark(self, pyspark_script):
        self._set_spark_external_urls()

        jars = list(self.config.jars)
        if self.config.main_jar:
            jars += [self.config.main_jar]

        return self._run_spark_submit(file=pyspark_script, jars=jars)

    def run_spark(self, main_class):
        self._set_spark_external_urls()
        return self._run_spark_submit(file=self.config.main_jar, jars=self.config.jars)

    def _report_step_status(self, step):
        logger.info(self._get_step_banner(step))

    def _get_step_banner(self, step):
        raise NotImplementedError()


class EmrLivyCtr(EmrCtrl, _LivySparkCtrl):
    def get_livy_endpoint(self):
        if self.emr_config.ssh_mode:
            cluster_dns = "localhost"
        else:
            cluster_dns = self.emr_cluster.get_public_dns()

        return Endpoint("http://%s:%s" % (cluster_dns, self.emr_config.livy_port))
