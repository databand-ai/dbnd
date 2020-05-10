import logging
import re

import six

from airflow.models import BaseOperator

from databand import dbnd_config
from dbnd import dbnd_handle_errors
from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.current import try_get_databand_context
from dbnd._core.decorator.schemed_result import ResultProxyTarget
from dbnd._core.utils.json_utils import convert_to_safe_types
from dbnd._core.utils.object_utils import safe_isinstance
from dbnd_airflow_operator.airflow_utils import (
    DbndFunctionalOperator_1_10_0,
    is_airflow_support_template_fields,
    safe_get_context_manager_dag,
)
from dbnd_airflow_operator.dbnd_functional_operator import DbndFunctionalOperator
from dbnd_airflow_operator.xcom_target import XComResults, XComStr, build_xcom_str
from targets import FileTarget, target


try:
    from inspect import signature
except ImportError:
    from funcsigs import signature

logger = logging.getLogger(__name__)

if not is_airflow_support_template_fields():
    DbndFunctionalOperator = DbndFunctionalOperator_1_10_0


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


@dbnd_handle_errors(exit_on_error=False)
def build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs):
    dag = safe_get_context_manager_dag()
    dag_ctrl = DagFuncOperatorCtrl.build_or_get_dag_ctrl(dag)
    return dag_ctrl.build_airflow_operator(
        task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
    )


_default_dbnd_dag_context_config = parse_and_build_config_store(
    source="airflow_defaults",
    config_values={"log": {"disabled": True, "capture_task_run_log": False}},
)


class DagFuncOperatorCtrl(object):
    dag_to_context = {}

    def __init__(self, dag):
        self.dag = dag
        self.dbnd_airflow_name = {}
        config_store = self.get_and_process_dbnd_dag_config()
        with dbnd_config(
            config_values=config_store, source="airflow"
        ) as current_config:
            self.dbnd_context = DatabandContext(name="airflow__%s" % self.dag.dag_id)
            with DatabandContext.context(_context=self.dbnd_context):
                # we need databand context to update config first
                self.dbnd_config_layer = current_config.config_layer

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
                value = build_xcom_str(value)
                upstream_task_ids.append(value.task_id)
            if isinstance(value, XComStr):
                upstream_task_ids.append(value.task_id)
                return target("xcom://%s" % value)
            if self._is_jinja_arg(value):
                return target("jinja://%s" % value)
            return value

        call_kwargs["task_name"] = af_task_id = self.get_normalized_airflow_task_id(
            call_kwargs.pop("task_name", task_cls.get_task_family())
        )
        call_args = [_process_xcom_value(arg) for arg in call_args]
        call_kwargs = {
            name: _process_xcom_value(arg) for name, arg in six.iteritems(call_kwargs)
        }

        task = task_cls(*call_args, **call_kwargs)  # type: Task

        # we will want to not cache "pipelines", as we need to run ".band()" per DAG
        setattr(task, "_dbnd_no_cache", True)

        user_inputs_only = task._params.get_param_values(
            user_only=True, input_only=True
        )
        # take only outputs that are coming from ctror ( based on ParameterValue in task.task_meta
        user_ctor_outputs_only = []
        for p_val in task.task_meta.task_params:
            if (
                p_val.parameter.is_output()
                and p_val.source
                and p_val.source.endswith("[ctor]")
            ):
                user_ctor_outputs_only.append((p_val.parameter, p_val.value))

        user_ctor_outputs_only_names = {
            p_val_def.name for p_val_def, p_val in user_ctor_outputs_only
        }
        dbnd_xcom_inputs = []
        dbnd_task_params_fields = []
        non_templated_fields = []
        dbnd_task_params = {}
        for p_def, p_value in user_inputs_only + user_ctor_outputs_only:
            p_name = p_def.name

            dbnd_task_params_fields.append(p_name)
            if isinstance(p_value, FileTarget) and p_value.fs_name == "xcom":
                dbnd_xcom_inputs.append(p_name)
                p_value = p_value.path.replace("xcom://", "")
            elif isinstance(p_value, FileTarget) and p_value.fs_name == "jinja":
                p_value = p_value.path.replace("jinja://", "")
                if p_def.disable_jinja_templating:
                    non_templated_fields.append(p_name)
            dbnd_task_params[p_name] = convert_to_safe_types(p_value)

        single_value_result = False
        if task.task_definition.single_result_output:
            if isinstance(task.result, ResultProxyTarget):
                dbnd_xcom_outputs = task.result.names
            else:
                dbnd_xcom_outputs = ["result"]
                single_value_result = True
        else:
            dbnd_xcom_outputs = [
                p.name
                for p in task._params.get_params(output_only=True, user_only=True)
            ]
        dbnd_xcom_outputs = [n for n in dbnd_xcom_outputs]

        op_kwargs = task.task_airflow_op_kwargs or {}
        allowed_kwargs = signature(BaseOperator.__init__).parameters
        for kwarg in op_kwargs:
            if kwarg not in allowed_kwargs:
                raise AttributeError(
                    "__init__() got an unexpected keyword argument '{}'".format(kwarg)
                )

        op = DbndFunctionalOperator(
            task_id=af_task_id,
            dbnd_task_type=task.get_task_family(),
            dbnd_task_id=task.task_id,
            dbnd_xcom_inputs=dbnd_xcom_inputs,
            dbnd_xcom_outputs=dbnd_xcom_outputs,
            dbnd_overridden_output_params=user_ctor_outputs_only_names,
            dbnd_task_params_fields=dbnd_task_params_fields,
            params=dbnd_task_params,
            **op_kwargs
        )
        # doesn't work in airflow 1_10_0
        op.template_fields = [
            f for f in dbnd_task_params_fields if f not in non_templated_fields
        ]
        task.ctrl.airflow_op = op

        if task.task_retries is not None:
            op.retries = task.task_retries
            op.retry_delay = task.task_retry_delay
        # set_af_operator_doc_md(task_run, op)

        for upstream_task in task.task_dag.upstream:
            upstream_operator = dag.task_dict.get(upstream_task.task_name)
            if not upstream_operator:
                self.__log_task_not_found_error(op, upstream_task.task_name)
                continue
            if not upstream_operator.downstream_task_ids:
                op.set_upstream(upstream_operator)

        for task_id in upstream_task_ids:
            upstream_task = dag.task_dict.get(task_id)
            if not upstream_task:
                self.__log_task_not_found_error(op, task_id)
                continue
            op.set_upstream(upstream_task)

        # populated Operator with current params values
        for k, v in six.iteritems(dbnd_task_params):
            setattr(op, k, v)

        results = [(n, build_xcom_str(op=op, name=n)) for n in dbnd_xcom_outputs]

        for n, xcom_arg in results:
            if n in user_ctor_outputs_only_names:
                continue
            setattr(op, n, xcom_arg)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                task.ctrl.banner("Created task '%s'." % task.task_name, color="green")
            )
            logger.debug(
                "%s\n\tparams: %s\n\toutputs: %s",
                task.task_id,
                dbnd_task_params,
                results,
            )
        if single_value_result:
            result = results[0]
            return result[1]  # return result XComStr
        return XComResults(result=build_xcom_str(op), sub_results=results)

    def _is_jinja_arg(self, p_value):
        jinja_regex = re.compile("{\s*{.*}\s*}")
        # Use regex match and not find to prevent false-positives
        # Finds two opening braces and two closing braces, ignoring whitespaces between them
        return isinstance(p_value, six.string_types) and jinja_regex.match(p_value)

    def get_and_process_dbnd_dag_config(self):
        dag = self.dag
        if not dag.default_args:
            dag_dbnd_config = {}
        else:
            dag_dbnd_config = dag.default_args.get("dbnd_config", {})

        config_store = parse_and_build_config_store(
            source="%s default args" % dag.dag_id,
            config_values=dag_dbnd_config,
            auto_section_parse=True,
        )

        # config can have problems around serialization,
        # let override with "normalized" config
        if dag.default_args:
            dag.default_args["dbnd_config"] = config_store

        config_store = _default_dbnd_dag_context_config.merge(config_store)
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

    def __log_task_not_found_error(self, op, task_id):
        logging.error(
            "Failed to connect to child %s %s: all known tasks are '%s'",
            op,
            task_id,
            self.dag.task_dict.keys(),
        )
