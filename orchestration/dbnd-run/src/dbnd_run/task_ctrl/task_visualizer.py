# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.constants import SystemTaskName
from dbnd._core.task_ctrl.task_visualiser import (
    FormatterVerbosity,
    _f_env,
    _f_none_default,
    _TaskBannerBuilder,
)
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.basics.text_banner import TextBanner
from dbnd_run.task.task import Task


class _TaskOrchestrationBannerBuilder(_TaskBannerBuilder):
    """
    Describe how to build a banner for different scenarios.
    Also describe how to build each of the columns in the banner.
    todo: can be extracted to builder and director
    """

    def __init__(
        self,
        task: "Task",
        msg: str,
        color: str,
        verbose: bool = False,
        print_task_band: bool = False,
    ):
        super().__init__(task=task, msg=msg, color=color, verbose=verbose)
        self.task: "Task" = task
        self.print_task_band = print_task_band
        self.is_driver_or_submitter = (
            self.task.task_name in SystemTaskName.driver_and_submitter
        )

    def build_orchestration_banner(
        self, task_run: "TaskRun" = None, exc_info=None
    ) -> TextBanner:
        """building the banner for orchestration scenario"""

        self.add_task_info()

        if not self.task.ctrl.should_run():
            self.add_disabled_info()

        if task_run:
            self.add_time_info(task_run)
            self.add_tracker_info(task_run)
            self.add_task_run_info(task_run)

            if task_run.external_resource_urls:
                self.add_external_resource_info(task_run)

            self.add_task_run_executor_log_info(task_run)

        all_info = self.verbosity >= FormatterVerbosity.HIGH
        self.table_director.add_params_table(
            all_params=all_info,
            param_format=True,
            param_source=True,
            param_section=all_info,
            param_default=all_info,
        )

        if self.verbosity >= FormatterVerbosity.HIGH:
            self._add_verbose_info()

        elif self.print_task_band:
            self._add_task_band_info()

        self.task._task_banner(self.banner, verbosity=self.verbosity)
        self.add_stack_and_errors(exc_info, task_run, self.verbosity)

        return self.banner.get_banner_str()

    def add_task_run_executor_log_info(self, task_run):
        log_manager = task_run.task_run_executor.log_manager
        logs = [("local", log_manager.local_log_file)]
        if log_manager.remote_log_file:
            logs.append(("remote", log_manager.remote_log_file))
        self.banner.column("LOG", self.banner.f_simple_dict(logs))

    def add_external_resource_info(self, task_run):
        self.banner.column_properties(
            "EXTERNAL", [(k, v) for k, v in task_run.external_resource_urls.items()]
        )

    def add_task_run_info(self, task_run):
        self.banner.column_properties(
            "TASK RUN",
            [
                ("task_run_uid", task_run.task_run_uid),
                ("task_run_attempt_uid", task_run.task_run_attempt_uid),
                ("state", task_run.task_run_state),
            ],
        )

    def add_task_info(self):
        from dbnd_run.task.task import DEFAULT_CLASS_VERSION

        task_params = [
            ("task_id", self.task.task_id),
            ("task_version", self.task.task_version),
        ]

        if self.task.task_class_version != DEFAULT_CLASS_VERSION:
            task_class_version = (
                _f_none_default(self.task.task_class_version, DEFAULT_CLASS_VERSION),
            )
            task_params.append(("task_class_version", task_class_version))

        task_params.extend(
            [
                ("env", self.task.task_env.task_name),
                ("env_cloud", self.task.task_env.cloud_type),
                ("env_label", _f_env(self.task.task_env.env_label)),
                ("task_target_date", self.task.task_target_date),
            ]
        )
        self.banner.column("TASK", self.banner.f_simple_dict(task_params))

    def add_disabled_info(self):
        disable = [
            ("task_enabled", self.task.task_enabled),
            ("task_enabled_in_prod", self.task.task_enabled_in_prod),
        ]

        self.banner.column("DISABLED", self.banner.f_simple_dict(disable))

    def add_time_info(self, task_run):
        time_fields = [("start", "%s" % task_run.start_time)]

        if task_run.finished_time:
            time_fields.append(("finished", "%s" % task_run.finished_time))
            task_duration = task_run.finished_time - task_run.start_time
            time_fields.append(("duration", "%s" % task_duration))

        self.banner.column("TIME", self.banner.f_simple_dict(time_fields))

    def _add_verbose_info(self):
        relations = self.task.ctrl.relations
        self.banner.new_section()
        self.banner.column("INPUTS USER", self.banner.f_io(relations.task_inputs_user))
        self.banner.column(
            "INPUTS SYSTEM", self.banner.f_struct(relations.task_inputs_system)
        )
        self.banner.new_section()

        task = self.task
        self.banner.column("TASK_BAND", task.task_band)
        self.banner.column(
            "OUTPUTS USER", self.banner.f_io(relations.task_outputs_user)
        )
        self.banner.column(
            "OUTPUTS SYSTEM", self.banner.f_struct(relations.task_outputs_system)
        )
        self.banner.write("\n")

        self.banner.column("SIGNATURE", task.task_signature_obj.signature)
        self.banner.column("SIGNATURE SOURCE", task.task_signature_obj.signature_source)
        if (
            task.task_outputs_signature_obj
            and task.task_outputs_signature_obj != task.task_signature_obj
        ):
            self.banner.column(
                "TASK OUTPUTS SIGNATURE", task.task_outputs_signature_obj.signature
            )
            self.banner.column(
                "TASK OUTPUTS SIGNATURE SOURCE",
                task.task_outputs_signature_obj.signature_source,
            )

    def _add_task_band_info(self):
        self.banner.new_section()
        self.banner.column("TASK_BAND", self.task.task_band)
