# © Copyright Databand.ai, an IBM Company 2022

from dbnd_docker.docker_ctrl import DockerRunCtrl


class LocalDockerRunCtrl(DockerRunCtrl):
    def __init__(self, **kwargs):
        super(LocalDockerRunCtrl, self).__init__(**kwargs)
        self.runner_op = None

    @property
    def docker_config(self):
        # type: (LocalDockerRunCtrl) -> DockerEngineConfig
        return self.task.docker_engine

    def docker_run(self):
        from dbnd_run.airflow.dbnd_airflow_contrib import DockerOperator

        t = self.task
        dc = self.docker_config
        environment = dc.environment.copy()
        environment.update(
            self.task_run.task_run_executor.run_executor.get_context_spawn_env()
        )

        self.runner_op = DockerOperator(
            task_id=self.task.task_id,
            image=t.image,
            command=t.command,
            docker_conn_id=dc.docker_conn_id,
            cpus=dc.cpus,
            environment=environment,
            api_version=dc.api_version,
            docker_url="unix://var/run/docker.sock",
            force_pull=dc.force_pull,
            mem_limit=dc.mem_limit,
            network_mode=dc.network_mode,
            tls_ca_cert=dc.tls_ca_cert,
            tls_client_cert=dc.tls_client_cert,
            tls_client_key=dc.tls_client_key,
            tls_hostname=dc.tls_hostname,
            tls_ssl_version=dc.tls_ssl_version,
            tmp_dir=dc.tmp_dir,
            user=dc.user,
            volumes=dc.volumes,
            working_dir=dc.working_dir,
            xcom_push=False,
            xcom_all=False,
            dns=dc.dns,
            dns_search=dc.dns_search,
            auto_remove=dc.auto_remove,
            shm_size=dc.shm_size,
        )

        self.runner_op.execute(context=None)

    def on_kill(self):
        if self.runner_op is not None:
            self.runner_op.on_kill()
