# © Copyright Databand.ai, an IBM Company 2022

import typing

from collections import Counter

from dbnd._core.constants import DescribeFormat, SystemTaskName, TaskRunState
from dbnd._core.current import is_verbose
from dbnd._core.run.run_ctrl import RunCtrl
from dbnd._core.tracking.schemas.tracking_info_objects import TaskRunEnvInfo
from dbnd._core.utils.basics.text_banner import TextBanner


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun


class RunBanner(RunCtrl):
    @property
    def tracker(self):
        return self.run.tracker

    def _add_tasks_info(self, b):
        run = self.run
        reused = sum(
            tr.is_reused and not tr.is_skipped_as_not_required for tr in run.task_runs
        )
        optimizations = []
        if reused:
            optimizations.append(
                "There are {completed} reused tasks.".format(completed=reused)
            )
        task_skipped_as_not_required = sum(
            tr.is_reused and tr.is_skipped_as_not_required for tr in run.task_runs
        )
        if task_skipped_as_not_required:
            optimizations.append(
                " {skipped} tasks are not required by any uncompleted task "
                "that is essential for your root task.".format(
                    skipped=task_skipped_as_not_required
                )
            )
        show_more = is_verbose()
        task_runs = run.task_runs
        if not show_more:  # show only non system
            task_runs = run.get_task_runs(without_system=True)
            if not task_runs:  # we have only system
                task_runs = run.task_runs

        states = Counter(tr.task_run_state for tr in task_runs if tr.task_run_state)

        b.column("TOTAL TASKS", len(task_runs))

        tasks = [(k.value, v) for k, v in states.items()]
        b.column_properties("STATES", tasks)
        if optimizations:
            b.column("RUN OPTIMIZATION", " ".join(optimizations))
        return b

    def run_banner(
        self, msg, color="white", show_run_info=False, show_tasks_info=False
    ):
        run = self.run  # type: DatabandRun
        if run.root_run_info.root_run_uid != run.run_uid:
            msg += " -> sub-run"

        b = TextBanner(msg, color)
        ctx = run.context
        task_run_env = ctx.task_run_env  # type: TaskRunEnvInfo

        if run.context.tracking_store.has_tracking_store("api", channel_name="web"):
            b.column("TRACKER URL", run.run_url, skip_if_empty=True)

        b.column("TRACKERS", run.context.tracking_store.trackers_names)

        if run.root_run_info.root_run_uid != run.run_uid:
            b.column(
                "ROOT TRACKER URL", run.root_run_info.root_run_url, skip_if_empty=True
            )
            b.column("ROOT UID URL", run.root_run_info.root_run_uid, skip_if_empty=True)

        if run.scheduled_run_info:
            b.column_properties(
                "SCHEDULED",
                [
                    ("scheduled_job", run.scheduled_run_info.scheduled_job_uid),
                    ("scheduled_date", run.scheduled_run_info.scheduled_date),
                    ("dag_run_id", run.scheduled_run_info.scheduled_job_dag_run_id),
                ],
            )

        if show_run_info:
            b.new_line()
            run_params = [
                ("user", task_run_env.user),
                ("run_uid", "%s" % run.run_uid),
                ("env", run.env.name),
                ("project", run.project_name) if run.project_name else None,
                ("user_code_version", task_run_env.user_code_version),
            ]
            run_params = list(filter(None, run_params))
            b.column("RUN", b.f_simple_dict(run_params))
            b.column("CMD", task_run_env.cmd_line)

            if task_run_env.user_data and task_run_env.user_data != "None":
                b.column("USER DATA", task_run_env.user_data, skip_if_empty=True)
            b.new_line()

        if run.is_orchestration:
            run_executor = run.run_executor
            driver_task_run = run.driver_task_run
            if show_run_info:
                if driver_task_run and driver_task_run.log:
                    b.column(
                        "LOG",
                        b.f_simple_dict(
                            [
                                ("local", driver_task_run.log.local_log_file),
                                ("remote", driver_task_run.log.remote_log_file),
                            ],
                            skip_if_empty=True,
                        ),
                    )

                b.column(
                    "EXECUTE",
                    b.f_simple_dict(
                        [
                            ("TASK_EXECUTOR", run_executor.task_executor_type),
                            ("PARALLEL", run_executor.parallel),
                            ("SUBMIT_DRIVER", run_executor.submit_driver),
                            ("SUBMIT_TASKS", run_executor.submit_tasks),
                        ],
                        skip_if_empty=True,
                    ),
                    skip_if_empty=True,
                )
            if run_executor.run_executor_type == SystemTaskName.driver:
                if run.root_task_run:
                    b.column("TASK_BAND", run.root_task_run.task.task_band)

                if show_tasks_info:
                    self._add_tasks_info(b)
                failed_task_runs = [
                    task_run
                    for task_run in run.get_task_runs()
                    if task_run.task_run_state == TaskRunState.FAILED
                ]
                if failed_task_runs:
                    f_msg = "\n".join(tr.task.task_id for tr in failed_task_runs)
                    b.column("FAILED", f_msg)
        b.new_line()

        return b.getvalue()

    def run_banner_for_submitted(self):
        msg = (
            "Your run has been submitted! Please check submitted job in UI\n"
            " * Use '--interactive' to have blocking run (wait for driver completion)\n"
            " * Use '--local-driver' (env.submit_driver=False) to run your driver locally\n"
        )
        return self.run_banner(
            msg, color="green", show_run_info=False, show_tasks_info=False
        )


def print_tasks_tree(root_task, task_runs, describe_format=DescribeFormat.short):
    from dbnd._core.task_ctrl.task_dag_describe import DescribeDagCtrl

    completed = {tr.task.task_id: tr.is_reused for tr in task_runs}
    run_describe_dag = DescribeDagCtrl(
        root_task, describe_format, complete_status=completed
    )
    run_describe_dag.tree_view(describe_format=describe_format)
