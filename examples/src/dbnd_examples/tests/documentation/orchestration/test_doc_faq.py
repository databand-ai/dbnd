#### DOC START

from dbnd import current_task, task


@task
def calculate_alpha(alpha: int = 0.5):
    return current_task().task_env.name  # The environment of the task
    # See EnvConfig object for all properties


#### DOC END


class TestDocFAQ:
    def test_doc(self):
        calculate_alpha.task().dbnd_run()
