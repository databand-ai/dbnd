# © Copyright Databand.ai, an IBM Company 2022
import contextlib
import logging

from dbnd._core.current import try_get_current_task_run
from dbnd._core.log import dbnd_log_debug, dbnd_log_exception
from dbnd.providers.spark.dbnd_spark_init import _safe_get_jvm_view


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def jvm_context_manager(target_task):
    """
    This context manager handles external JVM context.
    When task is started we need to explicitly tell JVM
    about it, so it will report metrics to the proper task.
    When task is exited, we need to get context back to the parent task.
    Consider this example:

        # before this task will start, we instruct JVM that current context is changed
        @task
        def parent_task():
            # before starting nested task need we switch context to the nested
            nested_task()
            # after nested task was completed, we need to switch context back to the parent

        @task
        def nested_task():
            # ...
    """

    parent_task = try_get_current_task_run()

    try_to_revert_to_parent = set_jvm_context(target_task)
    try:
        yield
    finally:
        if (
            try_to_revert_to_parent  # we have previously set
            and parent_task  # parent task exists
            and parent_task != target_task  # and it's not the same one
        ):
            set_jvm_context(parent_task)


def set_jvm_context(task_run):
    """
    When pyspark is called on the first place we want to ensure that spark listener will report metrics
    to the proper task. To achieve this, we directly set current context to our JVM wrapper.
    """
    try:
        jvm = _safe_get_jvm_view()
        if jvm is None:
            return False
        jvm_dbnd = jvm.ai.databand.DbndWrapper

        from py4j import java_gateway

        if isinstance(jvm_dbnd, java_gateway.JavaPackage):
            dbnd_log_debug(
                "java DbndWrapper is not loading, skipping Java Context injection"
            )
            # if DbndWrapper class is not loaded then agent or IO listener is not attached
            return False
        try:
            dbnd_log_debug("Set external JVM DbndWrapper context with current run")
            jvm_dbnd.instance().setExternalTaskContext(
                str(task_run.run.run_uid),
                str(task_run.task_run_uid),
                str(task_run.task_run_attempt_uid),
                str(task_run.task_af_id),
            )
            return True
        except Exception as jvm_ex:
            dbnd_log_exception(
                "Failed to set DBND context to JVM during DbndWrapper call: %s", jvm_ex
            )
    except Exception as ex:
        dbnd_log_exception("Failed to set DBND context to JVM: %s", ex)
    return False
