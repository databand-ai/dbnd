import logging

from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from dbnd._core.constants import CloudType
from dbnd._core.current import get_settings
from dbnd._core.utils.structures import list_of_strings
from dbnd_gcp.dataproc.dataproc_config import DataprocConfig
from dbnd_spark.spark_ctrl import SparkCtrl
from targets import target


logger = logging.getLogger(__name__)


class DataProcCtrl(SparkCtrl):
    def __init__(self, task_run):
        super(DataProcCtrl, self).__init__(task_run=task_run)

        self.dataproc = self.task.dataproc

        gcp_conn_id = self.task_env.conn_id
        self.cluster_hook = DataProcHook(gcp_conn_id=gcp_conn_id)
        self.cluster_info = self.cluster_hook.get_cluster(
            project_id=self.cluster_hook.project_id,
            region=self.dataproc.region,
            cluster_name=self.dataproc.cluster,
        )
        self.storage = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)

        cluster_temp = self.cluster_info.get("config", {}).get("configBucket")
        if cluster_temp:
            self.remote_sync_root = target("gs://%s/dbnd/sync" % cluster_temp)

    def _get_job_builder(self, job_type):
        job_builder = self.cluster_hook.create_job_template(
            self.task.task_id,
            self.dataproc.cluster,
            job_type=job_type,
            properties=self.config.conf,
        )
        # we will have "unique" job name by set_job_name
        job_builder.set_job_name(self.job.job_name)
        job_builder.add_args(list_of_strings(self.task.application_args()))
        job_builder.add_file_uris(self.deploy.sync_files(self.config.files))
        return job_builder

    def _run_job_builder(self, job_builder):
        self.cluster_hook.submit(
            self.cluster_hook.project_id, job_builder.build(), self.dataproc.region
        )

    def run_spark(self, main_class):
        job_builder = self._get_job_builder(job_type="sparkJob")
        jars = list(self.config.jars)
        # we expect SparkTask to behave like spark_submit api i.e.
        # main_jar is a jar to run, main_class is needed only if jar has no default main.
        # dataproc expects main_jar to have default main, so when both main_jar and main_class are set we
        # need to move main_jar to jars.
        if self.task.main_class:
            jars.append(self.config.main_jar)
            job_builder.set_main(None, self.task.main_class)
        else:
            job_builder.set_main(self.deploy.sync(self.config.main_jar), None)

        job_builder.add_jar_file_uris(self.deploy.sync_files(jars))

        return self._run_job_builder(job_builder)

    def run_pyspark(self, pyspark_script):
        job_builder = self._get_job_builder(job_type="pysparkJob")
        jars = list(self.config.jars)

        if self.config.main_jar:
            jars.append(self.config.main_jar)

        job_builder.add_jar_file_uris(self.deploy.sync_files(jars))
        job_builder.set_python_main(self.deploy.sync(pyspark_script))

        return self._run_job_builder(job_builder)

    @classmethod
    def create_engine(cls):
        from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
        from airflow.contrib.operators import dataproc_operator

        from dbnd._core.current import get_settings

        cloud = get_settings().get_env_config(CloudType.gcp)

        gcp_conn_id = cloud.conn_id

        dataproc_config = DataprocConfig()
        cluster_hook = DataProcHook(gcp_conn_id=gcp_conn_id)

        return dataproc_operator.DataprocClusterCreateOperator(
            task_id="create_dataproc_cluster",
            project_id=cluster_hook.project_id,
            cluster_name=dataproc_config.cluster,
            gcp_conn_id=gcp_conn_id,
            num_workers=dataproc_config.num_workers,
            zone=dataproc_config.zone,
            network_uri=dataproc_config.network_uri,
            subnetwork_uri=dataproc_config.subnetwork_uri,
            tags=dataproc_config.tags,
            storage_bucket=dataproc_config.storage_bucket,
            init_actions_uris=dataproc_config.init_actions_uris,
            init_action_timeout=dataproc_config.init_action_timeout,
            metadata=dataproc_config.metadata,
            image_version=dataproc_config.image_version,
            properties=dataproc_config.properties,
            master_machine_type=dataproc_config.master_machine_type,
            master_disk_size=dataproc_config.master_disk_size,
            worker_machine_type=dataproc_config.worker_machine_type,
            worker_disk_size=dataproc_config.worker_disk_size,
            num_preemptible_workers=dataproc_config.num_preemptible_workers,
            labels=dataproc_config.labels,
            delegate_to=dataproc_config.delegate_to,
            service_account=dataproc_config.service_account,
            service_account_scopes=dataproc_config.service_account_scopes,
            idle_delete_ttl=dataproc_config.idle_delete_ttl,
            auto_delete_time=dataproc_config.auto_delete_time,
            auto_delete_ttl=dataproc_config.auto_delete_ttl,
        )

    @classmethod
    def terminate_engine(cls):
        from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
        from airflow.contrib.operators import dataproc_operator

        dataproc_config = DataprocConfig()

        gcp_conn_id = get_settings().get_env_config(CloudType.gcp).conn_id

        cluster_hook = DataProcHook(gcp_conn_id=gcp_conn_id)
        delete_cluster = dataproc_operator.DataprocClusterDeleteOperator(
            task_id="delete_dataproc_cluster",
            cluster_name=dataproc_config.cluster,
            project_id=cluster_hook.project_id,
            gcp_conn_id=gcp_conn_id,
            region=dataproc_config.region,
        )

        return delete_cluster

    @classmethod
    def get_engine_policy(cls):
        return DataprocConfig().policy
