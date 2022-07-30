# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl


class DockerRunCtrl(TaskRunCtrl):
    def docker_run(self):
        pass

    def on_kill(self):
        pass
