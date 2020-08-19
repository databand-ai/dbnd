import typing

from collections import Counter

from dbnd._core.constants import RunState, SystemTaskName, TaskRunState, UpdateSource
from dbnd._core.current import is_verbose
from dbnd._core.run.run_ctrl import RunCtrl
from dbnd._core.settings.core import CoreConfig
from dbnd._core.tracking.schemas.tracking_info_objects import TaskRunEnvInfo
from dbnd._core.utils.basics.text_banner import TextBanner


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun


class DescribeRun(RunCtrl):
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
            task_runs = [tr for tr in run.task_runs if not tr.task.task_is_system]
            if not task_runs:
                task_runs = run.task_runs

        states = Counter(tr.task_run_state for tr in task_runs if tr.task_run_state)

        tasks = [("total", len(task_runs))]
        tasks += [(k.value, v) for k, v in states.items()]

        b.column_properties("TASKS", tasks)
        if optimizations:
            b.column("RUN OPTIMIZATION", " ".join(optimizations))
        return b

    def run_banner(self, msg, color="white", show_run_info=False, show_tasks_info=True):
        b = TextBanner(msg, color)
        run = self.run  # type: DatabandRun
        ctx = run.context
        task_run_env = ctx.task_run_env  # type: TaskRunEnvInfo
        driver_task = run.driver_task_run.task

        orchestration_mode = run.source == UpdateSource.dbnd

        b.column("TRACKER URL", run.run_url, skip_if_empty=True)
        b.column("TRACKERS", CoreConfig().tracker)
        if show_tasks_info and orchestration_mode and driver_task.is_driver:
            self._add_tasks_info(b)

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
            ]
            b.column("RUN", b.f_simple_dict(run_params))
            b.column(
                "LOG",
                b.f_simple_dict(
                    [
                        ("local", driver_task.local_driver_log),
                        ("remote", driver_task.remote_driver_root),
                    ]
                ),
            )
            b.column("USER CODE VERSION", task_run_env.user_code_version)
            b.column("CMD", task_run_env.cmd_line)

            if orchestration_mode:
                b.column(
                    "EXECUTE",
                    b.f_simple_dict(
                        [
                            ("TASK_EXECUTOR", run.task_executor_type),
                            ("PARALLEL", run.parallel),
                            ("SUBMIT_DRIVER", run.submit_driver),
                            ("SUBMIT_TASKS", run.submit_tasks),
                        ],
                        skip_if_empty=True,
                    ),
                    skip_if_empty=True,
                )
            if task_run_env.user_data and task_run_env.user_data != "None":
                b.column("USER DATA", task_run_env.user_data, skip_if_empty=True)
            b.new_line()

        failed_task_runs = [
            task_run
            for task_run in run.task_runs
            if task_run.task_run_state in TaskRunState.direct_fail_states()
        ]
        if failed_task_runs:
            f_msg = "\n\t".join(tr.task.task_id for tr in failed_task_runs)
            b.column("FAILED", f_msg)

        if run.root_task_run:
            b.column("TASK_BAND", run.root_task_run.task.task_band)

        b.new_line()

        return b.getvalue()

    def run_banner_for_finished(self):
        if self.run._run_state == RunState.SUCCESS:
            root_task_run = self.run.root_task_run
            root_task = root_task_run.task
            return "\n%s\n%s\n" % (
                root_task.ctrl.banner(
                    "Main task '%s' is ready!" % root_task.task_name,
                    color="green",
                    task_run=root_task_run,
                ),
                self.run_banner(
                    "Your run has been successfully executed!", color="green"
                ),
            )

        return self.run_banner("Your run has failed!", color="red", show_run_info=True)

    def run_banner_for_submitted(self):
        msg = (
            "Your run has been submitted! Please check submitted job in UI\n"
            " * Use '--interactive' to have blocking run (wait for driver completion)\n"
            " * Use '--local-driver' (env.submit_driver=False) to run your driver locally\n"
        )
        return self.run_banner(
            msg, color="green", show_run_info=False, show_tasks_info=False
        )

    def get_error_banner(self):
        # type: ()->str
        err_banners = []
        run = self.run
        err_banners.append(self.run_banner_for_finished())

        failed_task_runs = []
        for task_run in run.task_runs:
            if (
                task_run.last_error
                or task_run.task_run_state in TaskRunState.direct_fail_states()
            ):
                failed_task_runs.append(task_run)

        if len(failed_task_runs) > 1:
            # clear out driver task, we don't want to print it twice
            failed_task_runs = [
                tr
                for tr in failed_task_runs
                if tr.task.task_name not in SystemTaskName.driver_and_submitter
            ]

        for task_run in failed_task_runs:
            if task_run.task_run_state == TaskRunState.CANCELLED:
                msg_header = "Task has been terminated!"
            else:
                msg_header = "Task has failed!"
            msg = task_run.task.ctrl.banner(
                msg=msg_header, color="red", task_run=task_run
            )
            err_banners.append(msg)

        err_banners.append(
            self.run_banner(
                "Your run has failed! See more info above.",
                color="red",
                show_run_info=False,
            )
        )
        return u"\n".join(err_banners)
