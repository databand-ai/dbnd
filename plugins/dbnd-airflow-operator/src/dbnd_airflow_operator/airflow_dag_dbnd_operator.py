# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import logging
import sys

from subprocess import list2cmdline
from typing import List

import six

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from more_itertools import unique_everseen

from databand import dbnd_config
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.current import try_get_databand_context
from dbnd._core.decorator.schemed_result import ResultProxyTarget
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.utils.json_utils import convert_to_safe_types
from dbnd._core.utils.object_utils import safe_isinstance
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid
from dbnd_airflow_operator.airflow_utils import safe_get_context_manager_dag
from dbnd_airflow_operator.xcom_target import XComResults, XComStr
from targets import FileTarget, target


logger = logging.getLogger(__name__)


def is_in_airflow_dag_build_context():
    """
    :return: bool:  true if we are in DAG definition mode
    """
    context_manager_dag = safe_get_context_manager_dag()
    if not context_manager_dag:
        # there is no active DAG, no DAG context exists
        return False

    # dbnd code captures inline operator creation with Catcher, it's not in airflow mode.
    return not safe_isinstance(context_manager_dag, "DatabandOpCatcherDag")


def build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs):
    from airflow import settings

    dag = safe_get_context_manager_dag()
    dag_ctrl = FunctionalOperatorsDagCtrl.build_or_get_dag_ctrl(dag)
    return dag_ctrl.build_airflow_operator(
        task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
    )


default_dag_config = parse_and_build_config_store(
    source="airflow_defaults", config_values={"log": {"disabled": True}}
)


class FunctionalOperatorsDagCtrl(object):
    dag_to_context = {}

    def __init__(self, dag):
        self.dag = dag
        self.dbnd_airflow_name = {}
        config_store = self.get_and_process_dbnd_dag_config()
        with dbnd_config(
            config_values=config_store, source="airflow"
        ) as current_config:
            self.dbnd_config_layer = current_config.config_layer
            self.dbnd_context = DatabandContext(name="airflow__%s" % self.dag.dag_id)

    def build_airflow_operator(self, task_cls, call_args, call_kwargs):
        if try_get_databand_context() is self.dbnd_context:
            # we are already in the context of build
            return self._build_airflow_operator(
                task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
            )

        # we are coming from external world
        with dbnd_config.config_layer_context(
            self.dbnd_config_layer
        ) as c, DatabandContext.context(_context=self.dbnd_context) as dc:
            return self._build_airflow_operator(
                task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
            )

    def _build_airflow_operator(self, task_cls, call_args, call_kwargs):
        dag = self.dag
        upstream_task_ids = []

        # we support first level xcom values only (not nested)
        def _process_xcom_value(value):
            if isinstance(value, BaseOperator):
                value = build_xcom_str_from_op(value)
                upstream_task_ids.append(value.task_id)
            if isinstance(value, XComStr):
                upstream_task_ids.append(value.task_id)
                return target("xcom://%s" % value)
            return value

        call_kwargs["task_name"] = af_task_id = self.get_normalized_airflow_task_id(
            call_kwargs.pop("task_name", task_cls.get_task_family())
        )
        call_args = [_process_xcom_value(arg) for arg in call_args]
        call_kwargs = {
            name: _process_xcom_value(arg) for name, arg in six.iteritems(call_kwargs)
        }

        task = task_cls(*call_args, **call_kwargs)  # type: Task
        setattr(task, "_dbnd_no_cache", True)
        dc = try_get_databand_context()
        logger.debug("Environment: %s", dc.env)
        op_kwargs = task.task_airflow_op_kwargs or {}

        user_inputs_only = task._params.get_param_values(
            user_only=True, input_only=True
        )
        dbnd_xcom_inputs = []
        dbnd_task_params_fields = []
        dbnd_task_params = {}
        for p_def, p_value in user_inputs_only:
            p_name = p_def.name

            dbnd_task_params_fields.append(p_name)
            if isinstance(p_value, FileTarget) and p_value.fs_name == "xcom":
                dbnd_xcom_inputs.append(p_name)
                p_value = p_value.path.replace("xcom://", "")
            dbnd_task_params[p_name] = convert_to_safe_types(p_value)

        single_result = False
        if task.task_definition.single_result_output:
            if isinstance(task.result, ResultProxyTarget):
                dbnd_xcom_outputs = task.result.names
            else:
                dbnd_xcom_outputs = ["result"]
                single_result = True
        else:
            dbnd_xcom_outputs = [
                p.name
                for p in task._params.get_params(output_only=True, user_only=True)
            ]

        """
        Workaround for backwards compatibility with Airflow 1.10.0,
        dynamically creating new operator class (that inherits AirflowDagDbndOperator).
        This is used because XCom templates for each operator are found in a class attribute
        and we have different XCom templates for each INSTANCE of the operator.
        Also, the template_fields attribute has a different value for each task, so we must ensure that they
        don't get mixed up.
        """
        new_op = type(af_task_id, (AirflowDagDbndOperator,), {})
        op = new_op(
            task_id=af_task_id,
            dbnd_task_type=task.get_task_family(),
            dbnd_task_id=task.task_id,
            dbnd_xcom_inputs=dbnd_xcom_inputs,
            dbnd_xcom_outputs=dbnd_xcom_outputs,
            dbnd_task_params_fields=dbnd_task_params_fields,
            params=dbnd_task_params,
            **op_kwargs
        )
        task.ctrl.airflow_op = op

        for k, v in six.iteritems(dbnd_task_params):
            setattr(op, k, v)

        results = [(n, build_xcom_str(task, n)) for n in dbnd_xcom_outputs]
        for n, xcom_arg in results:
            setattr(op, n, xcom_arg)
        setattr(op, "dbnd_xcom_outputs", dbnd_xcom_outputs)

        if task.task_retries is not None:
            op.retries = task.task_retries
            op.retry_delay = task.task_retry_delay
        # set_af_operator_doc_md(task_run, op)

        for t_child in task.task_meta.children:
            # let's reconnect to all internal tasks
            t_child = try_get_databand_context().task_instance_cache.get_task_by_id(
                t_child
            )
            upstream_task = dag.task_dict.get(t_child.task_name)
            if not upstream_task:
                logging.error(
                    "Failed to connect to child %s %s: %s",
                    op,
                    t_child.task_name,
                    dag.task_dict.keys(),
                )
                continue
            op.set_upstream(upstream_task)

        for task_id in upstream_task_ids:
            upstream_task = dag.task_dict.get(task_id)
            if not upstream_task:
                logging.error("Failed to connect to dependency %s %s", op, task_id)
                continue
            op.set_upstream(upstream_task)

        # we are in inline debug mode -> we are going to execute the task
        # we are in the band
        # and want to return result of the object

        logger.debug("%s params: %s", task.task_id, dbnd_task_params)
        logger.debug("%s outputs: %s", task.task_id, results)
        if single_result:
            return results[0][1]
        return XComResults(results)

    def get_and_process_dbnd_dag_config(self):
        dag = self.dag
        if not dag.default_args:
            dag_dbnd_config = {}
        else:
            dag_dbnd_config = dag.default_args.get("dbnd_config", {})

        config_store = parse_and_build_config_store(
            source="%s default args" % dag.dag_id, config_values=dag_dbnd_config
        )

        # config can have problems around serialization,
        # let override with "normalized" config
        if dag.default_args:
            dag.default_args["dbnd_config"] = config_store

        config_store = default_dag_config.merge(config_store)
        logger.debug("Config store for %s: %s", self.dag.dag_id, config_store)
        return config_store

    def get_normalized_airflow_task_id(self, task_name):
        """
        we want to keep airflow id simple,
        if this is the first time we see this task_name, let's keep the original value,
        otherwise start to add _(idx) to the task_name
        """

        name_count = self.dbnd_airflow_name
        if task_name not in name_count:
            name_count[task_name] = 0
        else:
            name_count[task_name] += 1
            task_name = "%s_%d" % (task_name, name_count[task_name])
        logger.debug("normalized : %s", name_count)
        return task_name

    @classmethod
    def build_or_get_dag_ctrl(cls, dag):
        # dags can have same name (in tests), can be refreshed from disk..
        dag_key = id(dag)
        if dag_key not in cls.dag_to_context:
            cls.dag_to_context[dag_key] = cls(dag=dag)

        return cls.dag_to_context[dag_key]


class AirflowDagDbndOperator(BaseOperator):
    """
    This is the Airflow operator that is created for every Databand Task


    it assume all tasks inputs coming from other airlfow tasks are in the format

    """

    ui_color = "#ffefeb"

    @apply_defaults
    def __init__(
        self,
        dbnd_task_type,
        dbnd_task_id,
        dbnd_xcom_inputs,
        dbnd_xcom_outputs,
        dbnd_task_params_fields,
        **kwargs
    ):
        template_fields = kwargs.pop("template_fields", None)
        super(AirflowDagDbndOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id

        self.dbnd_task_params_fields = dbnd_task_params_fields
        self.dbnd_xcom_inputs = dbnd_xcom_inputs
        self.dbnd_xcom_outputs = dbnd_xcom_outputs

        # make a copy
        all_template_fields = list(self.template_fields)  # type: List[str]
        if template_fields:
            all_template_fields.extend(template_fields)
        all_template_fields.extend(self.dbnd_task_params_fields)

        self.__class__.template_fields = list(unique_everseen(all_template_fields))
        # self.template_fields = self.__class__.template_fields

    @property
    def task_type(self):
        # we want to override task_type so we can have unique types for every Databand task
        v = getattr(self, "_task_type", None)
        if v:
            return v
        return BaseOperator.task_type.fget(self)

    def execute(self, context):
        logger.info("Running dbnd task from airflow operator %s", self.task_id)

        new_kwargs = {}
        for p_name in self.dbnd_task_params_fields:
            new_kwargs[p_name] = getattr(self, p_name, None)
            # this is the real input value after
            if p_name in self.dbnd_xcom_inputs:
                new_kwargs[p_name] = target(new_kwargs[p_name])

        dag = context["dag"]
        dag_ctrl = FunctionalOperatorsDagCtrl.build_or_get_dag_ctrl(dag)
        with DatabandContext.context(_context=dag_ctrl.dbnd_context) as dc:
            logger.info("Running %s with kwargs=%s ", self.task_id, new_kwargs)
            dbnd_task = dc.task_instance_cache.get_task_by_id(self.dbnd_task_id)
            with dbnd_task.ctrl.task_context(phase=TaskContextPhase.BUILD):
                task = dbnd_task.clone(**new_kwargs)
                task._task_submit()

        logger.info("Finished to run %s", self)
        result = {
            output_name: convert_to_safe_types(getattr(task, output_name))
            for output_name in self.dbnd_xcom_outputs
        }
        return result

    def on_kill(self):
        from dbnd_airflow.dbnd_task_executor.dbnd_execute import dbnd_operator__kill

        return dbnd_operator__kill(self)


def _dbnd_track_and_execute(task, airflow_op, context):
    dag = context["dag"]
    dag_id = dag.dag_id
    run_uid = get_job_run_uid(dag_id=dag_id, execution_date=context["execution_date"])
    task_run_uid = get_task_run_uid(run_uid, airflow_op.task_id)

    dag_task = build_dag_task(dag)
    dc = try_get_databand_context()
    # create databand run
    with new_databand_run(
        context=dc,
        task_or_task_name=dag_task,
        run_uid=run_uid,
        existing_run=False,
        job_name=dag.dag_id,
    ) as dr:  # type: DatabandRun
        root_task_run_uid = get_task_run_uid(run_uid, dag_id)
        dr._init_without_run(root_task_run_uid=root_task_run_uid)

        # self._start_taskrun(dr.driver_task_run)
        # self._start_taskrun(dr.root_task_run)

        tr = dr.create_dynamic_task_run(
            task, dr.local_engine, _uuid=task_run_uid, task_af_id=airflow_op.task_id
        )
        tr.runner.execute(airflow_context=context)


def build_xcom_str(task, name):
    op = task.ctrl.airflow_op
    xcom_path = "{{task_instance.xcom_pull('%s')['%s']}}" % (op.task_id, name)
    return XComStr(xcom_path, op.task_id)


def build_xcom_str_from_op(op):
    xcom_path = "{{task_instance.xcom_pull('%s')}}" % (op.task_id)
    return XComStr(xcom_path, op.task_id)


def build_dag_task(dag):
    # type: (DAG) -> Task

    # create "root task" with default name as current process executable file name

    class InplaceTask(Task):
        _conf__task_family = dag.dag_id

    try:
        if dag.fileloc:
            dag_code = open(dag.fileloc, "r").read()
            InplaceTask.task_definition.task_source_code = dag_code
            InplaceTask.task_definition.task_module_code = dag_code
    except Exception:
        pass

    dag_task = InplaceTask(task_version="now", task_name=dag.dag_id)
    dag_task.task_is_system = True
    dag_task.task_meta.task_command_line = list2cmdline(sys.argv)
    dag_task.task_meta.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)
    return dag_task
