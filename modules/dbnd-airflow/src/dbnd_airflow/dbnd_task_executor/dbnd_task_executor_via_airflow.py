import contextlib
import json
import logging
import os
import typing

from airflow import DAG, configuration as conf, jobs, settings
from airflow.configuration import conf as airflow_conf
from airflow.executors import LocalExecutor, SequentialExecutor
from airflow.models import TaskInstance
from airflow.utils.db import provide_session
from airflow.utils.log import logging_mixin
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from backports.configparser import NoSectionError
from sqlalchemy.orm import Session

from dbnd._core.errors import friendly_error
from dbnd._core.plugin.dbnd_plugins import assert_plugin_enabled
from dbnd._core.settings import DatabandSettings, RunConfig
from dbnd._core.task_executor.task_executor import TaskExecutor
from dbnd_airflow.config import AirflowFeaturesConfig, get_dbnd_default_args
from dbnd_airflow.db_utils import remove_listener_by_name
from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
    AirflowDagAsDbndTask,
    AirflowOperatorAsDbndTask,
)
from dbnd_airflow.dbnd_task_executor.converters import operator_to_to_dbnd_task_id
from dbnd_airflow.dbnd_task_executor.dbnd_dagrun import create_dagrun_from_dbnd_run
from dbnd_airflow.dbnd_task_executor.dbnd_task_to_airflow_operator import (
    build_dbnd_operator_from_taskrun,
    set_af_operator_doc_md,
)
from dbnd_airflow.executors import AirflowTaskExecutorType
from dbnd_airflow.executors.simple_executor import InProcessExecutor
from dbnd_airflow.scheduler.single_dag_run_job import SingleDagRunJob


if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AirflowTaskExecutor(TaskExecutor):
    def __init__(self, run, host_engine, target_engine, task_runs):
        super(AirflowTaskExecutor, self).__init__(
            run=run,
            host_engine=host_engine,
            target_engine=target_engine,
            task_runs=task_runs,
        )
        self.airflow_features = AirflowFeaturesConfig()
        self.airflow_dag = None  # type: DAG

    def run_airflow_task(self, args):
        """
        called by executors, interprocess communication:  databand run_task ...
        """
        import time

        # time.sleep(1000)
        log = LoggingMixin().log
        # Load custom airflow config
        if args.cfg_path:
            with open(args.cfg_path, "r") as conf_file:
                conf_dict = json.load(conf_file)

            if os.path.exists(args.cfg_path):
                os.remove(args.cfg_path)

            # Do not log these properties since some may contain passwords.
            # This may also set default values for database properties like
            # core.sql_alchemy_pool_size
            # core.sql_alchemy_pool_recycle
            for section, config in conf_dict.items():
                for option, value in config.items():
                    try:
                        conf.set(section, option, value)
                    except NoSectionError:
                        log.error(
                            "Section {section} Option {option} "
                            "does not exist in the config!".format(
                                section=section, option=option
                            )
                        )

            settings.configure_vars()

        # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
        # behind multiple open sleeping connections while heartbeating, which could
        # easily exceed the database connection limit when
        # processing hundreds of simultaneous tasks.
        settings.configure_orm(disable_connection_pool=True)
        try:
            af_task = self.airflow_dag.get_task(task_id=args.task_id)
        except Exception:
            logger.exception(
                "Failed to get af_task %s from dag! See below for all known tasks:\n",
                args.task_id,
            )
            for t in self.airflow_dag.tasks:
                from dbnd._core.constants import DescribeFormat

                logger.warning(
                    self.run.root_task.ctrl.describe_dag._describe_task(
                        t, describe_format=DescribeFormat.verbose
                    )
                )

            raise

        ti = TaskInstance(af_task, args.execution_date)
        ti.refresh_from_db()

        logging_mixin.set_context(logging.root, ti)

        hostname = get_hostname()
        log.info("Running %s on host %s", ti, hostname)

        # we are in the dbnd simple mode
        if hasattr(af_task, "dbnd_task_id"):
            if self.airflow_features.task_run_simple:
                ti.run(
                    mark_success=args.mark_success, job_id=args.job_id, pool=args.pool
                )
                return

        if args.local:
            run_job = jobs.LocalTaskJob(
                task_instance=ti,
                mark_success=args.mark_success,
                pickle_id=args.pickle,
                ignore_all_deps=args.ignore_all_dependencies,
                ignore_depends_on_past=args.ignore_depends_on_past,
                ignore_task_deps=args.ignore_dependencies,
                ignore_ti_state=args.force,
                pool=args.pool,
            )
            run_job.run()
        elif args.raw:
            ti._run_raw_task(
                mark_success=args.mark_success, job_id=args.job_id, pool=args.pool
            )

    def build_airflow_dag(self, task_runs):
        # create new dag from current tasks and tasks selected to run
        root_task = self.run.root_task_run.task
        if isinstance(root_task, AirflowDagAsDbndTask):
            # it's the dag without the task itself
            dag = root_task.dag
            set_af_doc_md(self.run, dag)
            for af_task in dag.tasks:
                task_run = self.run.get_task_run(operator_to_to_dbnd_task_id(af_task))
                set_af_operator_doc_md(task_run, af_task)
            return root_task.dag

        dag = DAG(self.run.dag_id, default_args=get_dbnd_default_args())
        with dag:
            airflow_ops = {}
            for task_run in task_runs:
                task = task_run.task
                if isinstance(task, AirflowOperatorAsDbndTask):
                    op = task.airflow_op
                    # this is hack, we clean the state of the op.
                    # better : implement proxy object like
                    # databandOperator that can wrap real Operator
                    op._dag = dag
                    op.upstream_task_ids.clear()
                    dag.add_task(op)
                    set_af_operator_doc_md(task_run, op)
                else:
                    # we will create DatabandOperator for databand tasks
                    op = build_dbnd_operator_from_taskrun(task_run)

                airflow_ops[task.task_id] = op

            for task_run in task_runs:
                task = task_run.task
                op = airflow_ops[task.task_id]
                upstream_tasks = task.ctrl.task_dag.upstream
                for t in upstream_tasks:
                    if t.task_id not in airflow_ops:
                        # we have some tasks that were not selected to run, we don't add them to graph
                        continue
                    upstream_ops = airflow_ops[t.task_id]
                    if upstream_ops.task_id not in op.upstream_task_ids:
                        op.set_upstream(upstream_ops)

        dag.fileloc = root_task.task_definition.task_source_file
        set_af_doc_md(self.run, dag)
        return dag

    @provide_session
    def do_run(self, session=None):
        # type:  (Session) -> None
        af_dag = self.airflow_dag
        databand_run = self.run
        databand_context = databand_run.context
        execution_date = databand_run.execution_date
        s = databand_context.settings  # type: DatabandSettings
        s_run = s.run  # type: RunConfig

        airflow_task_executor = self._get_airflow_executor()

        if self.airflow_features.disable_db_ping_on_connect:
            from airflow import settings as airflow_settings

            try:
                remove_listener_by_name(
                    airflow_settings.engine, "engine_connect", "ping_connection"
                )
            except Exception as ex:
                logger.warning("Failed to optimize DB access: %s" % ex)

        if isinstance(airflow_task_executor, InProcessExecutor):
            heartrate = 0
        else:
            # we are in parallel mode
            heartrate = airflow_conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

        # "Amount of time in seconds to wait when the limit "
        # "on maximum active dag runs (max_active_runs) has "
        # "been reached before trying to execute a dag run "
        # "again.
        delay_on_limit = 1.0

        af_dag.sync_to_db(session=session)
        # let create relevant TaskInstance, so SingleDagRunJob will run them
        dag_run = create_dagrun_from_dbnd_run(
            databand_run=databand_run,
            af_dag=af_dag,
            execution_date=execution_date,
            session=session,
            state=State.RUNNING,
            external_trigger=False,
        )

        airflow_task_executor.fail_fast = s_run.fail_fast
        job = SingleDagRunJob(
            dag=dag_run.dag,
            execution_date=databand_run.execution_date,
            mark_success=s_run.mark_success,
            executor=airflow_task_executor,
            donot_pickle=(
                s_run.donot_pickle or airflow_conf.getboolean("core", "donot_pickle")
            ),
            ignore_first_depends_on_past=s_run.ignore_first_depends_on_past,
            ignore_task_deps=s_run.ignore_dependencies,
            fail_fast=s_run.fail_fast,
            pool=s_run.pool,
            delay_on_limit_secs=delay_on_limit,
            verbose=s.system.verbose,
            heartrate=heartrate,
            airflow_features=self.airflow_features,
        )

        # we need localDagJob to be available from "internal" functions
        # because of ti_state_manager use
        from dbnd._core.current import is_verbose

        with SingleDagRunJob.new_context(
            _context=job, allow_override=True, verbose=is_verbose()
        ):
            job.run()

    @contextlib.contextmanager
    def prepare_run(self):
        self.airflow_dag = dag = self.build_airflow_dag(task_runs=self.task_runs)

        task_original_dag = {}
        try:
            # money time  : we are running dag. let fix all tasks dags
            # in case tasks didn't have a proper dag
            for af_task in dag.tasks:
                task_original_dag[af_task.task_id] = af_task.dag
                af_task._dag = dag

            with super(AirflowTaskExecutor, self).prepare_run():
                yield dag
        finally:
            for af_task in dag.tasks:
                original_dag = task_original_dag.get(af_task.task_id)
                if original_dag:
                    af_task._dag = original_dag

    def validate_parallel_run_constrains(self):
        settings = self.run.context.settings
        using_sqlite = "sqlite" in settings.core.sql_alchemy_conn
        if not using_sqlite:
            return

        if settings.run.enable_concurent_sqlite:
            logger.warning(
                "You are running parallel execution on top of sqlite database! (see run.enable_concurent_sqlite)"
            )
            return

        # in theory sqlite can support a decent amount of parallelism, but in practice
        # the way airflow works each process holds the db exlusively locked which leads
        # to sqlite DB is locked exceptions
        raise friendly_error.execute_engine.parallel_or_remote_sqlite("parallel")

    def _get_airflow_executor(self):
        """Creates a new instance of the configured executor if none exists and returns it"""

        task_executor_type = self.task_executor_type
        task_engine = self.target_engine
        parallel = self.host_engine.parallel

        if task_executor_type == AirflowTaskExecutorType.airflow_inprocess:
            if parallel:
                raise friendly_error.execute_engine.parallel_with_inprocess(
                    task_executor_type
                )
            fail_fast = self.context.settings.run.fail_fast
            return InProcessExecutor(fail_fast=fail_fast)
        elif task_executor_type == AirflowTaskExecutorType.airflow_multiprocess_local:
            if parallel:
                self.validate_parallel_run_constrains()
                return LocalExecutor()
            return SequentialExecutor()

        elif task_executor_type == AirflowTaskExecutorType.airflow_kubernetes:
            from dbnd_airflow.executors.kubernetes_executor import (
                DbndKubernetesExecutor,
            )

            assert_plugin_enabled("dbnd-docker")
            self.validate_parallel_run_constrains()
            if (
                task_engine.task_executor_type
                != AirflowTaskExecutorType.airflow_kubernetes
            ):
                raise friendly_error.executor_k8s.kubernetes_with_non_compatible_engine(
                    task_engine
                )
            kube_dbnd = task_engine.build_kube_dbnd()
            return DbndKubernetesExecutor(kube_dbnd=kube_dbnd)

        from airflow.executors import _get_executor as _airflow_executor

        # do we need to make executor singleton? if we share it between multiple runs?
        logger.warning("Using default airflow executor %s", task_executor_type)
        return _airflow_executor(task_executor_type)


def set_af_doc_md(run, dag):
    dag.doc_md = (
        "### Databand Info\n"
        "* **Tracker**: [{0}]({0})\n"
        "* **Run Name**: {1}\n"
        "* **Run UID**: {2}\n".format(run.run_url, run.name, run.run_uid)
    )
